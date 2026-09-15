# Cryo: static bug audit, coverage gaps, and remediation plan

## Context

Three parallel static audits (Common/Database/Adaptors, SQLite/CloudKit backends, Resilient/Synchronized stores + tests) plus direct verification of the highest-severity claims produced ~200 findings. This plan (1) fixes the bugs in priority order and (2) makes the test suite actually run and cover the risky paths.

### What the app actually uses (drives priority)

| Cryo API | App call sites | Notes |
|---|---|---|
| `CloudSyncable` / `CloudSyncStores` | `Features/Progress/State/ProgressSync.swift`, `LessonsModeState.swift`, `Encouragement.swift` | Local = `DocumentAdaptor`, remote = `CloudKitAdaptor`. **Primary production sync path.** |
| `CloudKitAdaptor` (`connect`, `createTable`, `insert(replace:true)`, `select`, `delete`, `observeAvailabilityChanges`, `iCloudRecordID`) | `Services/Cloud/CloudModelStore.swift`, `Services/Persistence/StorageAdapters.swift` | |
| `DocumentAdaptor` (local + `cloud(config:subdirectory:)`) | `Services/Persistence/Persistence.swift` | |
| `UserDefaultsAdaptor`, `UbiquitousKeyValueStoreAdaptor` | `Persistence.swift`, `StorageAdapters.swift` | |
| `withCryoTimeout` | `Persistence.swift` (container + record-ID lookup) | |
| `@CryoPersisted`, `@CryoLocalDocument` | 8 + 4 refs | |
| `SQLiteAdaptor` | 37 refs (models via `@CryoColumn`) | |
| `SynchronizedStore`, `ResilientDatabaseStore` | **0 app refs** (only Cryo's own tests) | Lowest priority; see decision below |

### How tests are (not) run today

- `Tests/CryoTests` is **not** in `Mathical.xcodeproj` and no Makefile/CI target runs it. The only way is `swift test` inside `Mathical/Library/Cryo`.
- `swift test` on macOS silently skips `Database/CloudSyncable.swift` (`#if canImport(UIKit)`), i.e. the app's primary sync path is never even compiled by the package tests.
- Package.swift depends on Toolbox via git `branch: "dev"` (pinned in Package.resolved to `c321da6c`, the same revision as the vendored subtree). Under `MONOREPO` the import is compiled out.
- 27 test methods, all through `Mocks.swift` (`MockCloudKitAdaptor`, one `isAvailable` failure flag, no clock, reimplements query semantics so no real `CloudKit*Query` runs). Suites share `UserDefaultsAdaptor.shared`, `DocumentAdaptor.sharedLocal`, and one `_cryo_test.db` path.

## Part 1: Bug inventory (verified unless marked "reported")

Severity: **P0** = crash or silent data loss on a path the app uses; **P1** = crash/data loss on a Cryo path the app doesn't use, or wrong results; **P2** = smell / robustness.

### P0: paths the app uses

| # | File:line | Bug |
|---|---|---|
| 1 | `Database/Operation.swift:106-151` | `rowId: String?` encoded as JSON null for `.update`/`.delete`, decoded as non-optional `String` → id-less update/delete can never be decoded. `@CryoLocalDocument.init` swallows with `try?` → whole persisted queue silently dropped. |
| 2 | `SQLite/SQLiteAdaptor.swift:283-294` | Update-hook context block `unsafeBitCast` to raw pointer, never retained → use-after-free on first row change after `registerChangeListener`. |
| 3 | `SQLite/SQLiteAdaptor.swift:356-359` | `sqlite3_bind_blob(…, nil)` (static destructor) with a `withUnsafeBytes` pointer; bind happens in `compiledQuery()`, step later in `execute()` → dangling pointer for `Data`/array/dictionary columns. Empty `Data` binds NULL. |
| 4 | `SQLite/SQLiteAdaptor.swift:351,379` | `sqlite3_bind_int(Int32(value))` traps above 32 bits; `sqlite3_column_int` truncates reads. |
| 5 | `SQLite/SQLiteAdaptor.swift:421-422` | `fatalError` on `select` of any model with `@CryoAsset`. |
| 6 | `Database/Value.swift:247-259,302-314,366-378,399-413` | `Optional` conformances map nil → `0`/`""`/empty `Data` and read back `.some`; optional columns can never be nil on SQLite. `Optional<Date>` has no conformance → JSON blob in a TEXT column → row unreadable. CloudKit path unwraps separately → backends disagree. |
| 7 | `Database/Value.swift:241,244,299,332,380-386` | `Self(rawValue:)!` / `URL(string:)!` traps on unknown stored enum values or malformed URLs; string enums silently coerce unknown values to `.allCases.first!`. |
| 8 | `Database/Model.swift:165-229,277,294,365` | `CryoSchemaManager.shared`: `@MainActor` writes, unsynchronized reads from any executor; `schema(for:)` `fatalError`s instead of throwing `CryoError.schemaNotInitialized`; `try! Self(from: CryoEmptyDecoder())` traps for string-enum columns; traps if `id` is not a stored `@CryoColumn`. |
| 9 | `Common/ModelDecoder.swift:26,30,116,131-143,159-163,266` | `fatalError` for `UInt64` and nested containers on the decode path. |
| 10 | `CloudKit/CloudKitAdaptor.swift:256-265` | `placeholderSymbol` returns `"%d"` for `.double` → predicate compares against a truncated integer. |
| 11 | `CloudKit/CloudKitInsertQuery.swift:100-121`, `CloudKitUpdateQuery.swift:201-218`, `CloudKitDeleteQuery.swift:108-112` | Per-record save/delete results inspected only under `#if DEBUG` for logging; `execute()` returns `true`/matched count regardless. `replace:false` duplicates are not reported (SQLite throws `duplicateId`). |
| 12 | `CloudKit/CloudKitSelectQuery.swift:122-126,144,156-190,193-214` | `select(id:)` ignores where/sort/limit and throws `unknownItem` where SQLite returns `[]`; `resultsLimit` not passed to CloudKit and only checked after page 2; column names interpolated into the predicate format (should be `%K`); `as! NSString` on a missing relation field. |
| 13 | `CloudKit/CloudKitUpdateQuery.swift:124-182,190` | Fetch duplicated from select without `cloudKitOperation` retry; `set` uses `recordValue` (no optional unwrap, no compression, no `CKAsset`) unlike insert. |
| 14 | `CloudKit/CloudKitCreateTableQuery.swift:57-72` | Body entirely inside `#if DEBUG`; in DEBUG inserts a `try!` dummy record and may leave it behind. |
| 15 | `Database/CloudSyncable.swift:349-376` | `cleanupOldInstances` keys the dictionary by `identifier` and compares `identifier != identifier` → never deletes anything. |
| 16 | `Database/CloudSyncable.swift:158-161,219-222,242-245,297-309` | Network/auth errors logged as "failed to decode" and treated as not-found; `loadInstance` then creates an empty instance and `saveLocally` overwrites good local data. |
| 17 | `Database/CloudSyncable.swift:136,140,238,165-194,259,66` | `recency`/`compare` truncate to whole seconds; self filter uses `identifierForVendor` while the app keys by its own device identifier; sync file I/O on `@MainActor`; `@MainActor` `Codable` conformers decoded on `Task.detached` in `DocumentAdaptor`. |
| 18 | `Adaptors/DocumentAdaptor.swift:164,158-175,221-229` | Non-atomic `data.write(to:)`; `NSFileCoordinator` calls commented out (including for the ubiquity container). |
| 19 | `Adaptors/DocumentAdaptor.swift:60,81,166,248-260,102-109` | `createDirectory(withIntermediateDirectories:false)` under `try?`; removing a missing key throws (other adaptors no-op); `removeAll()` with `subdirectory:nil` deletes the entire Documents dir; key id unsanitized and differs on iOS 16+ vs older. |
| 20 | `Adaptors/DocumentAdaptor.swift:418,483,534,556,541-569,596,612-627` | Block observers removed via `removeObserver(self,…)` → never removed, leaks, possible double continuation resume; `UbiquitousDocumentMetadata` reads `DownloadingStatusKey as? Bool` and `IsDownloadingKey as? String` (always nil), builds `URL(string:)` from a path; `isDownloaded` relies on `PercentDownloaded` which is absent for local files → download stream never finishes. |
| 21 | `Adaptors/UserDefaultsAdaptor.swift:41-92`, `Adaptors/UbiquitousAdaptor.swift:45-91` | Persist dispatches on the dynamic value type, load on `Key.Value.self` → optional `Key.Value` (`Date?`, `Int?`, …) written natively, read via the JSON branch → nil forever. |
| 22 | `Adaptors/UserDefaultsAdaptor.swift:99-104`, `UbiquitousAdaptor.swift:102-107` | `removeAll()` iterates `dictionaryRepresentation()` and wipes every key in the domain, including non-Cryo ones. |
| 23 | `Adaptors/UbiquitousAdaptor.swift:136,20-23,175-186` | Observers always registered on `NSUbiquitousKeyValueStore.default`, not the injected store; `observers` array on a shared singleton mutated without synchronization. |
| 24 | `Common/PropertyWrappers.swift:92,180,271,358,57,71,144,158,236,249,323,336` | Writes are fire-and-forget `Task`s: no ordering, errors discarded. Inits swallow load errors with `try?` → a corrupt file becomes the default and the next write destroys the original. |
| 25 | `Common/Timeout.swift:20-38` | Timer task not cancelled when the operation wins (lives for the full timeout); caller cancellation not propagated; `try? Task.sleep` would report a cancellation as a timeout; `UInt64(timeout * 1e9)` traps on `.infinity`. |
| 26 | `SQLite/SQLiteAdaptor.swift:58-65` | `transaction` / `withAttachedDatabase` are `// TODO` no-ops that never invoke the closure. |
| 27 | `SQLite/SQLite*Query.swift` (all five) | `execute()` finalizes the cached statement but never nils `queryStatement` → second `execute()` steps a finalized statement. |
| 28 | `SQLite/SQLiteSelectQuery.swift:267-269`, `SQLiteUpdateQuery.swift:216`, `SQLiteDeleteQuery.swift:184` | Clauses added after `queryString` is read are dropped (SQL memoized, `bind` return code ignored) → a DELETE can silently lose its WHERE. |
| 29 | `SQLite/SQLiteSelectQuery.swift:186,230` | `as! String` / `value!` trap on NULL FK or missing relation row. |
| 30 | `SQLite/SQLiteAdaptor.swift:363-364,393-406` | Dates bound via `ISO8601DateFormatter()` without fractional seconds (precision loss; breaks `date >` watermarks) and a new formatter per value. |

### P1: Resilient / Synchronized store (not used by the app)

| # | File:line | Bug |
|---|---|---|
| 31 | `ResilientStore/ResilientDatabaseStore.swift:140-141` | `execute(operation:enqueueIfFailed:)` appends to the queue without persisting (`saveOnWrite:false`). |
| 32 | `…:154-180` | Replay does remove+persist before deciding to re-append (crash window loses the op; a persist throw aborts `init`); partial failure reorders ops; retry budget off-by-one (`< max` checked before increment from 0). |
| 33 | `…:106,128-134`, `ResilientQueries.swift:33-36,69-72,118-122` | Plain class, no serialization → concurrent drains execute ops twice; every error swallowed into "queued". |
| 34 | `…:223-238` | `persist(operation:)`, `loadOperations`, `createTable` pass straight through → `SynchronizedStore` gets no resilience; offline writes are lost. |
| 35 | `…:79-104` | Query methods are `internal` on a `public` class. |
| 36 | `SynchronizedStore/SynchronizedStore.swift:246` | `catch { log(try operation.operation.description) }` re-throws inside the catch and aborts the batch. |
| 37 | `…:360-371` | `deviceIdentifier()` is the hardware model string → same-model devices share an id and never sync. |
| 38 | `…:204-214,197` | `reset()` wipes local data and replays only other devices' ops; `clear()` deletes the shared cloud operation log for all devices. |
| 39 | `…:239-249,231`, `SynchronizedStoreBackend.swift:44,83` | Watermark advances past failed ops (permanent loss); strict `>` drops equal-timestamp ops; watermark uses the publishing device's clock. |
| 40 | `…:223-228` | Single-record notification path lacks store/device filter and moves the watermark past unapplied ops. |
| 41 | `SQLite/SQLiteAdaptor.swift:167` | Replayed inserts use `replace:false` → upserts never propagate. |
| 42 | `SynchronizedStore/SynchronizedQueries.swift:30-35` | Local write commits before `publish`; publish failure leaves permanent divergence. |
| 43 | `SynchronizedStore.swift:85,174`, `SynchronizedStoreBackend.swift:121-132` | Store cannot be constructed offline; failed subscription save recorded as `changeSubscriptionSetup = true` forever. Retain cycle store → hooks → store. |

### P2: robustness / consistency (fix opportunistically within the phases above)

- `SQLiteAdaptor.swift:15` passes `absoluteString` (a `file://` URL) to `sqlite3_open`; no `sqlite3_busy_timeout`.
- `SQLiteCreateTableQuery.swift:65-84`: `IF NOT EXISTS` only, no migration; `id` is `UNIQUE` not `PRIMARY KEY` so `INSERT OR REPLACE` resets `_cryo_created`; identifiers unquoted.
- `SQLiteInsertQuery.swift:86-98` `logQueryString` overwrites `completeQueryString`; `:149-158` constraint classification by substring of `errmsg`.
- `CloudKitAdaptor.swift:346` vs `CloudKitUpdateQuery.swift:190`: `Data` lzfse-compressed on insert, raw on update; `:486-511` retry wrapper ignores `partialFailure` and drops the logger.
- `Query.swift:76-96`: WHERE cannot express NULL; `.asset` is never constructed (`URL` matched first); `and()` is a plain alias for `where()`; `MultiQuery` non-atomic; `CryoQueryResult`/`NoOpQuery` dead.
- `DatabaseAdaptor.swift:45-59,82-99`: half the protocol is `#if false`; `ensureAvailability` default no-op and `isAvailable` default `true`.
- `Adaptor.swift:63-74` `load(with:defaultValue:)` has a write side effect and lost-update race. `CryoError` carries `Any.Type` so it can't be `Sendable`/`Equatable`.
- `CryoEmptyDecoder.swift:52-66,107-121,153-167` three copy-pasted `decode<T>` bodies that can throw into `try!` callers.
- `Value.swift:343` malformed UUID silently becomes the zero UUID; `Value.swift:415-429` public blanket `dataValue` on every `Encodable`.
- `CloudSyncable.swift:172-193` "unsupported" (`nil`) and "failed" (`[]`) are swapped.
- Multiple `Sendable` violations that will fail under Swift 6 language mode (`CryoConfig.log`, adaptors captured in `Task`, `CloudSyncStores.localInstance`).

## Part 2: Test coverage gaps (summary)

Entirely untested: `CloudKitAdaptor` and all five `CloudKit*Query` classes (the mock reimplements them), `UbiquitousKeyValueStoreAdaptor`, `CloudSyncable.swift` (not even compiled on macOS), `SQLiteUpdateQuery` directly, `SQLiteAdaptor.transaction`/`registerChangeListener`, `DocumentAdaptor` beyond a flat round-trip (cloud, ubiquitous metadata, `listInstanceKeys`, subdirectory, `removeAll` scope), `CryoKeyValue`/`CryoUbiquitousKeyValue`/`CryoLocalDocument` wrappers, `DatabaseOperation` Codable round-trip, `@CryoOneToMany`, `CryoQueryValue` conformances, `SynchronizedStore`/`ResilientCloudKitStore` public wrappers, `reset()`/`clear()`, `deviceIdentifier()`.

Weak: only `Int16`/`String` column types on SQLite (no `Int`, `Data`, `Date`, `Bool`, `Double`, enum, UUID, URL, optionals); only `equals`/`>`/`<` operators; sort and limit never combined; no execute-twice / clause-after-queryString; no concurrency tests at all; no failure injection beyond a global on/off flag; no clock; 10 of 12 `CryoError` cases never asserted; timeout cancellation untested. The production `ResilientStoreImpl where Backend == CloudKitAdaptor` extension is never executed (tests run a mock duplicate).

## Part 3: Decisions taken (change these if you disagree)

1. **`SynchronizedStore` / `ResilientCloudKitStore` get deprecated, not redesigned.** The app has no consumer. Fix only the contained defects (Phase 6) and mark both `@available(*, deprecated)`. A correct sync needs a per-device monotonic sequence instead of a date watermark; that is a separate RFC.
2. **Tests run via `xcodebuild test` on the package with an iOS simulator destination**, wired as `make test-library-cryo` and a second CI job. Reason: an Xcode `CryoTests` target would need `@testable import Mathical` rewrites and hosted-app runs; `swift test` on macOS can't compile `CloudSyncable.swift`. `swift test` stays as the fast local loop.
3. **`CryoQueryValue` gains a `.null` case**; the `Optional: CryoColumn{Int,Double,String,Data}Value` conformances are deleted. Backends bind NULL / omit the field. Typed `.optional(inner:)` rejected as more invasive.
4. **`CryoSchemaManager` and `SQLiteAdaptor` stay classes guarded by locks**, not actors. Actors would force `await` through every synchronous SQLite builder the app calls.
5. **CloudKit queries gain SQLite parity**: `execute()` throws on per-record failure, `replace:false` duplicate → `CryoError.duplicateId`, missing id → `[]` / `0`.
6. **`DatabaseOperation.insert` carries a `replace` flag** (decoded as `true` when absent for old payloads).
7. **`cleanupOldInstances` is removed** (dead by construction: one record per device id). If pruning superseded devices is wanted, add an opt-in policy later.
8. **`DocumentAdaptor.removeAll()` refuses unscoped roots** (`subdirectory: nil`) via an `ownsDirectory` flag; key ids containing `/`, `\`, or `..` are rejected rather than rewritten.
9. **Property wrappers collapse to one implementation** behind the four existing names; writes are serialized through a per-wrapper task chain; load failures suppress auto-save instead of overwriting the corrupt file.

API-breaking changes (all in Cryo only; grep the app and fix call sites in the same PR): `.null` in exhaustive switches; removed `Optional` conformances; `init(integerValue:)`/`init(doubleValue:)`/`init(stringValue:)` become `throws`; `CryoModel.schema` and `CryoSchemaManager.schema(for:)` throw; `loadRemoteInstances(excludingIdentifier:)` replaces `includeSelf:`; CloudKit `execute()` throwing; `removeAll()` refusing unscoped roots; `CryoDatabaseAdaptor` losing default `isAvailable`/`ensureAvailability`; deprecations.

## Part 4: Fix phases (one PR each; regression tests land red-then-green in the same PR)

### Phase 1: test infrastructure and injection seams (no behaviour change)

- **Makefile** (`/Users/jonaszell/Developer/Mathical/Makefile`): add
  ```make
  CRYO_DIR := Mathical/Library/Cryo
  test-library-cryo: ## Run Cryo's package tests on the iOS Simulator
  	cd "$(CRYO_DIR)" && xcodebuild test -scheme Cryo -destination '$(DESTINATION)' \
  	  -derivedDataPath "$(DERIVED_DATA_PATH)/Cryo" $(TEST_OPTIONS) $(RESULT_BUNDLE_OPTION) \
  	  $(if $(SANITIZER),-enable$(SANITIZER)Sanitizer YES)
  test-library-cryo-fast: ## macOS subset via swift test (no CloudSyncable)
  	cd "$(CRYO_DIR)" && swift test $(if $(FILTER),--filter '$(FILTER)')
  ```
  Scheme `Cryo` is the shared scheme in `Cryo/.swiftpm/xcode/xcshareddata/xcschemes/`. Add `test-library-cryo` to `.PHONY` and `test-all`.
- **CI** (`.github/workflows/fast-tests.yml`): add a `cryo-tests` job running `make test-library-cryo` with the same `DEVELOPER_DIR`/`SIMULATOR`/`TEST_OPTIONS` (`-skipMacroValidation` needed for Toolbox macros), plus a matrix entry `SANITIZER=Address` (catches bugs 2 and 3). Toolbox is resolved from GitHub as pinned in `Package.resolved` (public repo, verified reachable).
- **`CloudKitAdaptor.swift`**: introduce `protocol CloudKitDatabase` (`record(for:)`, `records(matching:resultsLimit:)`, `records(continuingMatchFrom:resultsLimit:)`, `modifyRecords(saving:deleting:savePolicy:)`, `save(_ subscription:)`), conformed by `CKDatabase`; `database` becomes `any CloudKitDatabase`; add `internal init(config:database:userRecordID:)`. Cursor wrapped in `struct CloudKitQueryCursor { let token: Any }` since `CKQueryOperation.Cursor` has no public init. `cloudKitOperation` gets an injectable `sleep:` for retry tests. All `CloudKit*Query.swift` `database:` parameters change type (mechanical).
- **Clock**: `CryoConfig.now: @Sendable () -> Date = { Date() }`; `Operation.swift` gets `operation(now:)`; Resilient/Synchronized stores and insert queries read `config.now()`.
- **Store injection**: `ResilientCloudKitStoreConfig.queueStore` (default `DocumentAdaptor.sharedLocal`) and `SynchronizedStoreConfig.keyValueStore` (default `UserDefaultsAdaptor.shared`), used via `@CryoPersisted(adaptor:)` instead of the singleton-bound wrappers.
- **Metadata seam** (`DocumentAdaptor.swift`): `protocol UbiquitousMetadataItem { func value(forAttribute:) -> Any? }` (retroactive on `NSMetadataItem`) and `protocol UbiquitousMetadataQuerying` with an `AsyncStream` of gather/update events, so `NotificationCenter` plumbing lives in one adapter class.
- **`Tests/CryoTests/Support/`** (new): `CryoTestEnvironment` (per-test temp root, `UserDefaults(suiteName:)` removed in tearDown, unique sqlite path, `ManualClock`, `InMemoryCloudKitDatabase` + `CloudKitAdaptor` pair), `InMemoryCloudKitDatabase` (per-record `failures`, whole-call `nextOperationErrors` incl. `requestRateLimited` with `CKErrorRetryAfterKey`, `isOnline`, `pageSize` default 2, `fetchLog` of `(query, resultsLimit)`, `.ifServerRecordUnchanged` → `serverRecordChanged` for stale records; predicates evaluated with `predicate.evaluate(with:)` against `NSMutableDictionary` rows), `FakeMetadataQuery`, `FakeUbiquitousKeyValueStore` (subclass), `TestModels`.
- **Delete `Tests/CryoTests/Mocks.swift`**; the mock-specific `ResilientStoreImpl where Backend == MockCloudKitAdaptor` duplicate goes with it so the production `where Backend == CloudKitAdaptor` extension (`ResilientDatabaseStore.swift:183-216`) is what runs. Rewrite existing `setUp`s onto `CryoTestEnvironment`; `SQLiteTests.swift:57-60` stops using `absoluteString` as a path.
- Expose `static func makePredicate(id:whereClauses:)` on `UntypedCloudKitSelectQuery` (internal) for direct predicate tests. Add internal `CryoSchemaManager.reset()` for schema tests.

### Phase 2: value layer, operations, schema, decoders (bugs 1, 6, 7, 8, 9)

Files: `Database/Query.swift`, `Value.swift`, `Operation.swift`, `Model.swift`, `Common/ModelDecoder.swift`, `Common/CryoEmptyDecoder.swift`, `Common/Error.swift`.

- `Query.swift:12` add `case null` (encoded `{"null": true}`); `init(value:)` (`:438`) checks `_CryoOptionalValue` first: nil → `.null`, some → recurse. This also fixes `Optional<Date>`. Consumers of `columnValue` handle `.null` by binding NULL / setting the field to nil / using the schema column's `metaType.nilValue`.
- `Value.swift`: delete the four `Optional: CryoColumn*Value` extensions (`:247-259, 302-314, 366-378, 399-413`); make `init(integerValue:)`/`init(doubleValue:)`/`init(stringValue:)` `throws` in the protocols (`:39, 48, 57`; non-throwing conformers still satisfy them); `RawRepresentable` defaults (`:241, 244, 299, 385`) throw new `CryoError.invalidStoredValue(type:value:)` instead of `!` / `.allCases.first!` (drop the `CaseIterable` constraint); `URL` (`:332`) and `UUID` (`:343`) throw instead of trapping / zero-UUID.
- `Model.swift`: `CryoSchemaManager` drops `@MainActor`, guards both dictionaries with `OSAllocatedUnfairLock`; `schema(for:)`/`schema(tableName:)` throw `schemaNotInitialized`; `CryoModel.schema` becomes `get throws`; `try!` at `:277` and the `first { }!` lookups (`:314, 316, 328, 330`) and the id check (`:365`) throw `CryoError.invalidModel(message:)`. `CryoEmptyDecoder` returns `Type.defaultValue` for `_AnyCryoColumnValue` types so string-enum columns never go through `init(stringValue: "")`; collapse its three copy-pasted `decode<T>` bodies into one helper.
- `ModelDecoder.swift`: every `fatalError` (`:26, 30, 116, 131-143, 159-163, 266`) → `throw DecodingError.typeMismatch/dataCorrupted`; `UInt64` decodes from the `Int64` bit pattern when non-negative; type mismatches report `typeMismatch` not `keyNotFound`.
- `Operation.swift`: `decodeIfPresent` for `rowId` in `.update`/`.delete` (`:141, 151`); add `replace: Bool` to `.insert` (key `_4`, `decodeIfPresent ?? true`); add `var replace: Bool { get }` to `CryoInsertQuery` with a default of `true`. Fix `description` to include set-clause column names and join WHERE with ` AND `.

### Phase 3: launch-path adaptors, wrappers, timeout, CloudSyncable (bugs 15-25)

Files: `Adaptors/DocumentAdaptor.swift`, `UserDefaultsAdaptor.swift`, `UbiquitousAdaptor.swift`, `Common/PropertyWrappers.swift`, `Common/Timeout.swift`, `Database/CloudSyncable.swift`, and the app's `Features/Progress/State/ProgressSync.swift:46-56` for the `loadInstance` change.

- **DocumentAdaptor**: `.atomic` write (`:164`); reinstate `NSFileCoordinator` behind an injectable `coordinate` closure used only when `usesUbiquitousStorage` (`:158-175, 221-229`); `withIntermediateDirectories: true` and log the error (`:60, 81`); treat `fileNoSuchFile` on remove as success (`:166`); `ownsDirectory` flag + `removeAll()` refusing unscoped roots and never removing directories (`:248-260`); single `appendingPathComponent` path with id validation (`:102-109`); keep observer tokens and `removeObserver(token)`; `resumed` flag on the continuation and cleanup in `onTermination` (`:418-569`); metadata: `DownloadingStatusKey as? String` compared to `…StatusCurrent/Downloaded`, `IsDownloadingKey as? Bool`, `URL(fileURLWithPath:)`, `isDownloaded` from status not percent (`:596, 612-627`); use the injected `fileManager` for enumeration; break out of the loop after `finish(throwing:)` (`:408, 463`).
- **UserDefaults / Ubiquitous adaptors**: dispatch persist on `Key.Value.self` exactly like load (optional `Key.Value` → JSON branch on both sides); add `keyPrefix: String?` to both inits so `removeAll()` only touches prefixed keys (nil keeps legacy behaviour); register the KVS observer on the injected `store` (`UbiquitousAdaptor.swift:136`); lock around `observers`; log when `NSUbiquitousKeyValueStore.synchronize()` returns false.
- **PropertyWrappers**: implement `CryoKeyValue`/`CryoUbiquitousKeyValue`/`CryoLocalDocument` as thin wrappers over `CryoPersisted(adaptor:)`; per-wrapper `PersistenceQueue` (lock + task chain) so writes apply in order; `persist()` awaits the chain, add `flush()`; `onError` callback (default logs via `CryoConfig`); init records `loadError` and suppresses `saveOnWrite` until an explicit `persist()`.
- **Timeout**: `guard timeout.isFinite else { return try await operation() }`; keep the unstructured operation task (non-cooperative APIs must not block the caller) but track the timer task and cancel whichever loses; timer uses `try await Task.sleep` and returns silently on `CancellationError`; wrap in `withTaskCancellationHandler` cancelling both tasks and resolving with `CancellationError()`.
- **CloudSyncable**: remove `cleanupOldInstances` and its call (`:329, 349-376`); replace `UIDevice.cryoCurrentDeviceIdentifier` with `excludingIdentifier:` (`mergeWithRemoteInstances` passes `self.identifier`); `compare`/`recency` use full `Date` precision (`:136, 140`); `localInstance`/`remoteInstance` distinguish "missing" from "failed" via internal `Result`-returning helpers so `loadInstance` neither creates nor saves an empty instance after a load error, and add `loadInstanceReportingErrors` for the app; swap the `nil`/`[]` semantics at `:172-193`; `saveLocally` encodes on the main actor then awaits `stores.local.persist(data, url:)`; `localInstance` reads `Data` off-main (new `DocumentAdaptor.loadData(for:)`) and decodes on the main actor.

### Phase 4: CloudKit queries (bugs 10-14)

Files: `CloudKit/CloudKitAdaptor.swift`, `CloudKitSelectQuery.swift`, `CloudKitInsertQuery.swift`, `CloudKitUpdateQuery.swift`, `CloudKitDeleteQuery.swift`, `CloudKitCreateTableQuery.swift`.

- `placeholderSymbol` (`CloudKitAdaptor.swift:256-265`) always `%@`; column names via `%K` (`CloudKitSelectQuery.swift:144`, `CloudKitUpdateQuery.swift:150`).
- `select(id:)` (`CloudKitSelectQuery.swift:122-126`): catch `unknownItem` → `[]`, then apply where clauses client-side with the existing `CloudKitAdaptor.check(clause:object:)` and the limit; pass `resultsLimit` to `records(matching:resultsLimit:)`, check after every page, truncate; `as! NSString` (`:205`) → guard + `queryDecodeFailed`.
- Insert/update/delete inspect `saveResults`/`deleteResults` outside `#if DEBUG`, throw the first per-record error; `replace == false` + `serverRecordChanged` → `duplicateId`; delete treats `unknownItem` as 0; update returns saved count.
- `CloudKitUpdateQuery.fetch` (`:124-182`) deleted in favour of `UntypedCloudKitSelectQuery.fetch`; `set` (`:190`) looks up the schema column and uses `nsObject(from:column:)`; `.null` → `record[column] = nil`.
- `CloudKitCreateTableQuery.swift:57-72`: drop the `#if DEBUG` gate; deterministic dummy id `_cryo_schema_<table>` with `replace: true`, deleted in a step that runs even when the insert threw; `try!` → `try`.
- Retry wrapper (`:486-511`): forward `log`; retry per-record `requestRateLimited` inside `partialFailure`; add `networkUnavailable`/`zoneBusy`.

### Phase 5: SQLite (bugs 2-5, 26-30 and P2 SQLite items)

Files: `SQLite/SQLiteAdaptor.swift`, `SQLiteCreateTableQuery.swift`, `SQLiteInsertQuery.swift`, `SQLiteSelectQuery.swift`, `SQLiteUpdateQuery.swift`, `SQLiteDeleteQuery.swift`.

- Open with `sqlite3_open_v2(databaseUrl.path, …, READWRITE|CREATE|FULLMUTEX)`, `sqlite3_busy_timeout(5_000)`, `sqlite3_extended_result_codes(1)` (`:15`).
- Update hook (`:283-294`): store the block in `private var updateHookBox`, pass `Unmanaged.passUnretained(self).toOpaque()` as context, clear in `deinit`. Lock around `updateHooks`.
- `bind` (`:347-372`): `sqlite3_bind_int64`; blobs with `SQLITE_TRANSIENT`; empty `Data` → `sqlite3_bind_zeroblob(…, 0)`; `.null` → `sqlite3_bind_null`; check return codes. `columnValue` (`:375-424`): `sqlite3_column_int64`; `SQLITE_NULL` → nil → `metaType.nilValue`; `.asset` read/written as TEXT.
- Dates: one shared formatter with `.withFractionalSeconds` for writing; two-formatter fallback for reading (documented caveat: pre-fix whole-second rows sort after same-second fractional rows).
- `transaction` (`:58`): async mutex around `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK`, invoking the closure; `withAttachedDatabase` (`:63`): `ATTACH`/`DETACH` around the closure.
- Query builders: `execute()` nils `queryStatement` after finalize; `where/set/limit/sort` reset `completeQueryString` and throw `modifyingFinalizedQuery` only after compilation; remove `logQueryString` (`SQLiteInsertQuery.swift:86-98`); `as!`/`!` at `SQLiteSelectQuery.swift:186, 230` → throws.
- DDL: quote identifiers; after `CREATE TABLE IF NOT EXISTS` run `PRAGMA table_info` and `ALTER TABLE … ADD COLUMN` for missing columns; `INSERT … ON CONFLICT("id") DO UPDATE` to preserve `_cryo_created`; classify constraints via `sqlite3_extended_errcode` (`SQLiteInsertQuery.swift:149-158`); `execute(operation:)` (`SQLiteAdaptor.swift:167`) honours `replace`.

### Phase 6: Resilient / Synchronized stores, adaptor protocol (bugs 31-43, P2 protocol items)

- Deprecate `SynchronizedStore` and `ResilientCloudKitStore` publicly; note in `Documentation.docc/Cryo.md`.
- `ResilientStoreImpl` → `actor`; `execute(operation:enqueueIfFailed:)` persists after append (`:140-141`); `executeFailedOperations` increments and persists before executing, removes only on success or exhausted budget, keeps order, counts attempts as `retries + 1` (`:154-180`); `init` logs instead of aborting on persist failure; `persist(operation:)` and `createTable` (`:219-238`) go through the queue / defer offline; empty `catch {}` logs; query methods become `public` (`:79-104`).
- `SynchronizedStoreImpl`: `:246` `(try? …) ?? "<undecodable>"`; device identifier = UUID generated once and stored in the injected KV store (`:360-371`); watermark advances only past applied ops, failed ops retried next sync, `>=` plus a persisted set of applied op ids (`:231-249`); single-record path filters by store/device and doesn't move the watermark (`:223-228`); construct offline by deferring `createTable`/subscription (`:85, :174`); `changeSubscriptionSetup = true` only on success (`SynchronizedStoreBackend.swift:121-132`); `reset()` replays own history too; `clear()` clears local data + watermark only; `synchronize()` serialized; `[weak self]` in hook closures (`:178-182`).
- `DatabaseAdaptor.swift`: delete both `#if false` blocks; make `isAvailable`/`ensureAvailability` required.

## Part 5: New tests (by file; REG = fails before the fix, CHAR = characterization)

- **`DatabaseAdaptorContractTests`** (same cases on SQLite and CloudKit+fake): optional columns nil/some for `Int? Double? String? Date? Data? URL? UUID? enum?` (REG); `Int64` extremes and `UInt` above `Int32` (REG SQLite); 1 MB and empty `Data` (REG SQLite, run under ASan); sub-second dates (REG SQLite); unknown enum raw value and invalid URL throw (REG); array/dictionary columns (CHAR); `select(id:)` missing → `[]` and id+where (REG CloudKit); duplicate insert without replace → `duplicateId` (REG CloudKit); update/delete affected counts (REG CloudKit); all six operators on int/double/string/date (REG CloudKit double); limit+sort (REG CloudKit); unqualified update/delete (CHAR); execute twice on one query object (REG SQLite); `execute(operation:)` honours `replace` (REG both).
- **`ValueTests`**: nil → `.null`, some → inner case, `Optional<Date>` is `.date` (REG); `.null` Codable round-trip + legacy payload decodes (REG/CHAR); throwing RawRepresentable/URL/UUID inits (REG).
- **`OperationCodableTests`**: update/delete with nil `rowId` round-trip (REG, bug 1); insert `replace` flag + legacy payload → true (REG); `.null` in an operation (REG); `SyncOperation` data round-trip (CHAR).
- **`SchemaTests`**: uninitialized schema throws (REG); `createSchema` off-main and concurrent (REG, TSan); string-enum column model builds (REG); model without `id` column throws (REG); decoder nested container / `UInt64` / missing key (REG).
- **`SQLiteAdaptorTests`** (extend `SQLiteTests`): DB file created at `url.path` (REG); change listener survives 1000 row changes under ASan (REG, bug 2) and receives insert/update/delete (CHAR); where/limit/sort added after `queryString` still apply and DELETE keeps its WHERE (REG); clause after compile throws (CHAR); missing relation target / NULL FK throw (REG, in `SQLiteRelationTests`); asset column select doesn't trap (REG); transaction commits / rolls back on throw, attached DB closure invoked (REG); legacy whole-second date still readable (CHAR) and sub-second `>` works (REG); reserved-word identifiers (REG); create-table adds missing columns (REG); replace preserves `_cryo_created` (REG); busy timeout with a second connection (REG); concurrent queries (REG, TSan); insert SQL is parameterized (CHAR).
- **`CloudKitQueryTests`** (replaces `CloudKitTests`): full-type round-trip incl. asset (CHAR); limit passed to first fetch and stops when reached (REG); cursor pagination and sort (CHAR); `select(id:)` honours limit, missing relation field throws (REG); predicate uses `%K`, double not truncated, all operators evaluate, date comparison (REG/CHAR); insert per-record failure throws and `serverRecordChanged` → `duplicateId` (REG); update set nil/data/asset, retry on rate limit, partial failure count (REG); delete missing id → 0 and partial failure throws (REG); create-table inserts+removes dummy, removes stale dummy, skips when not initializing (REG/CHAR); retry honours `retryAfter` and gives up (CHAR); `backendNotAvailable` before connect, late connect upgrade, account-change reconnect (CHAR).
- **`ResilientStoreTests`** (rewritten on `CloudKitAdaptor` + fake): mirroring on/off (CHAR); enqueue persists across instances (REG); relaunch replays in order and partial failure keeps order (REG); op not lost when persist fails (REG); exact retry budget (REG); concurrent drains execute once (REG); corrupt queue reported not dropped (REG); nil-rowId update survives relaunch (REG, bug 1 end-to-end); `persist(operation:)` queued offline and init succeeds offline (REG).
- **`SynchronizedStoreTests`** (extend; injected clock/KV/documents): existing five (CHAR); offline device rejoining (CHAR); equal-timestamp ops not skipped, clock skew (REG); duplicate notification applies once, single-record path filters and doesn't move watermark (REG); failed op retried, undecodable op doesn't abort batch (REG); replace propagates as upsert, publish failure queues (REG); constructs offline, subscription failure retried (REG); reset replays own history, clear keeps shared log (REG); concurrent synchronize serialized, stable device id, store released (REG).
- **`CloudSyncableTests`** (new, `#if canImport(UIKit)`): merge never deletes remote records (CHAR, pins the decision); local load failure doesn't create+save an empty instance (REG); missing local creates new (CHAR); remote network error leaves local untouched (CHAR); sub-second recency (REG); self excluded by identifier (REG); merge picks higher recency and consolidates (CHAR); decode runs on the main actor (REG).
- **`DocumentAdaptorTests`** (document half of `LocalTests` moves here): round-trip (CHAR); atomic write / reader never sees a partial file (REG); corrupt file throws (CHAR); remove missing key no-op, nested subdirectory created (REG); `removeAll` refused on unscoped root and scoped to files (REG); `/` and `..` ids rejected (REG); `listInstanceKeys` (CHAR); ubiquitous writes go through the coordinator (REG); metadata status strings, file URL, current-without-percent is downloaded (REG); download stream finishes, observers removed on termination, continuation resumes once (REG).
- **`KeyValueAdaptorTests`** (UserDefaults + Ubiquitous via fake store): round-trips (CHAR); optional `Int?`/`Date?`/nil (REG both); `removeAll` prefixed only vs legacy (REG/CHAR); Float/URL/Data (CHAR); observer on injected store (REG), removed (CHAR), concurrent observe/remove (REG, TSan).
- **`PropertyWrapperTests`** (extend): writes persist in order with a slow adaptor, `flush()` waits (REG); write error reported, load error suppresses auto-save (REG); missing value uses default (CHAR); all four wrappers share behaviour (CHAR).
- **`TimeoutTests`** (extend): timer cancelled when operation wins (REG); caller cancellation cancels operation and throws `CancellationError` (REG); cancellation during timer isn't a timeout (REG); `.infinity` doesn't trap (REG); zero timeout times out, non-cooperative op doesn't delay caller (CHAR).

## Part 6: Verification

Fast local loop (macOS, excludes `CloudSyncable.swift`; first run fetches Toolbox + swift-syntax; `.build/` is already git-ignored):
```bash
cd /Users/jonaszell/Developer/Mathical/Mathical/Library/Cryo && swift test
```
Full suite on the simulator (includes CloudSyncable), plus sanitizer runs for the memory and concurrency regressions:
```bash
cd /Users/jonaszell/Developer/Mathical && make test-library-cryo SIMULATOR="iPhone 16 Pro"
```
```bash
cd /Users/jonaszell/Developer/Mathical && make test-library-cryo SIMULATOR="iPhone 16 Pro" SANITIZER=Address
```
```bash
cd /Users/jonaszell/Developer/Mathical && make test-library-cryo SIMULATOR="iPhone 16 Pro" SANITIZER=Thread
```
App still builds and its fast plan passes with the `MONOREPO` configuration (Cryo sources compile into the app):
```bash
cd /Users/jonaszell/Developer/Mathical && make build SIMULATOR="iPhone 16 Pro"
```
```bash
cd /Users/jonaszell/Developer/Mathical && make test-fast SIMULATOR="iPhone 16 Pro" TEST_OPTIONS="-enableCodeCoverage YES -skipMacroValidation CODE_SIGNING_ALLOWED=NO"
```
After each API-breaking phase, grep the app for changed symbols and fix call sites in the same PR:
```bash
cd /Users/jonaszell/Developer/Mathical && grep -rn "loadRemoteInstances\|CryoQueryValue\|\.schema\b\|removeAll()" Mathical --include='*.swift' | grep -v Library/
```
CI: the new `cryo-tests` job (and its ASan matrix entry) must be green on the PR for each phase. After merge, `make library-push-Cryo` pushes the subtree so upstream Cryo receives the same fixes and tests.