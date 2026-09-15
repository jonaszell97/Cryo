import CloudKit
import XCTest
@testable import Cryo

final class InjectionSeamTests: CryoTestCase {
    @MainActor func testSchemaResetClearsBothIndexes() throws {
        let manager = CryoSchemaManager()
        try manager.createSchema(for: SeamTestModel.self)
        XCTAssertNotNil(try manager.schema(tableName: SeamTestModel.tableName))
        manager.reset()
        XCTAssertThrowsError(try manager.schema(for: SeamTestModel.self))
        XCTAssertThrowsError(try manager.schema(tableName: SeamTestModel.tableName))
    }

    func testInjectedUnavailableAdaptorDoesNotConnectToAnAccount() async {
        let cloud = CloudKitAdaptor(config: .init(), database: environment.database, userRecordID: nil)
        XCTAssertFalse(cloud.isAvailable)
        let connected = await cloud.connect()
        XCTAssertFalse(connected)
    }

    func testEnvironmentIsolation() async throws {
        let other = try CryoTestEnvironment()
        defer { try? other.tearDown() }
        let key = CryoNamedKey(id: "same-key", for: Int.self)
        try await environment.documents.persist(7, for: key)
        try await environment.keyValueStore.persist(9, for: key)
        XCTAssertNil(try other.documents.loadSynchronously(with: key))
        XCTAssertNil(try other.keyValueStore.loadSynchronously(with: key))
        XCTAssertNotEqual(environment.sqliteURL, other.sqliteURL)
    }

    func testClockControlsQueriesAndOperations() async throws {
        let cloud = environment.cloud
        try await cloud.createTable(for: SeamTestModel.self, initializeCloudKitSchema: false).execute()
        let now = environment.clock.now()
        let query = try cloud.insert(SeamTestModel())
        XCTAssertEqual(query.untypedQuery.created, now)
        let operation = try await query.operation(now: now)
        guard case .insert(let date, _, _, _, _) = operation else { return XCTFail("Expected insert") }
        XCTAssertEqual(date, now)
        environment.clock.advance(by: 0.25)
        let sqlite = try SQLiteAdaptor(databaseUrl: environment.sqliteURL, config: environment.config)
        try await sqlite.createTable(for: SeamTestModel.self).execute()
        XCTAssertEqual(try sqlite.insert(SeamTestModel()).untypedQuery.created, now.addingTimeInterval(0.25))
    }

    func testProductionQueryPaginationAndSort() async throws {
        let cloud = environment.cloud
        try await cloud.createTable(for: SeamTestModel.self, initializeCloudKitSchema: false).execute()
        for index in 0..<5 { try await cloud.insert(SeamTestModel(id: "\(index)", value: "\(index)")).execute() }
        let result = try await cloud.select(from: SeamTestModel.self).sort(by: "value", .descending).execute()
        XCTAssertEqual(result.map(\.id), ["4", "3", "2", "1", "0"])
        XCTAssertEqual(environment.database.fetchLog.count, 1)
        XCTAssertEqual(environment.database.continuationLimits.count, 2)
        let query = CKQuery(recordType: SeamTestModel.tableName, predicate: NSPredicate(value: true))
        let page = try await environment.database.records(matching: query, resultsLimit: 1)
        XCTAssertEqual(page.matchResults.count, 1)
        XCTAssertNotNil(page.queryCursor)
    }

    func testFakeRejectsStaleRecordAndPreservesSnapshots() async throws {
        let database = environment.database
        let original = CKRecord(recordType: "Row", recordID: .init(recordName: "row"))
        original["value"] = "first" as NSString
        _ = try await database.modifyRecords(saving: [original], deleting: [], savePolicy: .ifServerRecordUnchanged)
        let first = try await database.record(for: original.recordID)
        let stale = try await database.record(for: original.recordID)
        first["value"] = "second" as NSString
        _ = try await database.modifyRecords(saving: [first], deleting: [], savePolicy: .ifServerRecordUnchanged)
        XCTAssertEqual(stale["value"] as? String, "first")
        let results = try await database.modifyRecords(saving: [stale], deleting: [], savePolicy: .ifServerRecordUnchanged)
        guard case .failure(let error) = results.saveResults[original.recordID] else { return XCTFail("Expected conflict") }
        XCTAssertEqual((error as? CKError)?.code, .serverRecordChanged)
    }

    func testFakeFailuresAndRetryClock() async throws {
        let database = environment.database
        let record = CKRecord(recordType: "Row")
        database.failures[record.recordID] = CKError(.permissionFailure)
        let results = try await database.modifyRecords(saving: [record], deleting: [], savePolicy: .changedKeys)
        guard case .failure(let error) = results.saveResults[record.recordID] else { return XCTFail("Expected per-record error") }
        XCTAssertEqual((error as? CKError)?.code, .permissionFailure)
        database.nextOperationErrors = [InMemoryCloudKitDatabase.rateLimit(retryAfter: 0.75)]
        let initial = environment.clock.now()
        let query = CKQuery(recordType: "Row", predicate: NSPredicate(value: true))
        var delays: [TimeInterval] = []
        _ = try await CloudKitAdaptor.cloudKitOperation(sleep: { delay in
            delays.append(delay)
            self.environment.clock.advance(by: delay)
        }) {
            try await database.records(matching: query, resultsLimit: 1)
        }
        XCTAssertEqual(delays, [0.75])
        XCTAssertEqual(environment.clock.now(), initial.addingTimeInterval(0.75))
        database.isOnline = false
        do {
            _ = try await database.record(for: record.recordID)
            XCTFail("Expected offline error")
        } catch { XCTAssertEqual((error as? CKError)?.code, .networkUnavailable) }
    }

    func testRetryBudgetUsesInjectedSleepOnEveryAttempt() async throws {
        var attempts = 0
        var delays: [TimeInterval] = []
        do {
            let _: Void = try await CloudKitAdaptor.cloudKitOperation(maxAttempts: 2, defaultDelay: 1, sleep: { delays.append($0) }) {
                attempts += 1
                throw CKError(.serviceUnavailable)
            }
            XCTFail("Expected retry exhaustion")
        } catch { XCTAssertEqual((error as? CKError)?.code, .serviceUnavailable) }
        XCTAssertEqual(attempts, 3)
        XCTAssertEqual(delays, [1, 2])
    }

    @MainActor func testMetadataGatherAndUpdatesUseInjectedQuery() async throws {
        let fake = FakeMetadataQuery()
        fake.initialEvents = [.gathered([])]
        var adaptor = DocumentAdaptor(config: .init(), url: environment.root, usesUbiquitousStorage: true)
        adaptor.makeMetadataQuery = { fake }
        let update = expectation(description: "update after initial results")
        let result = try await adaptor.loadUbiquitousDocuments(at: environment.root, onUpdate: { items in
            XCTAssertTrue(items.isEmpty)
            update.fulfill()
            return false
        })
        XCTAssertTrue(result.isEmpty)
        XCTAssertTrue(fake.started)
        fake.send(.updated([]))
        await fulfillment(of: [update], timeout: 1)
        XCTAssertTrue(fake.stopped)
    }

    @MainActor func testMetadataStartFailureAndEmptyDownload() async throws {
        let failed = FakeMetadataQuery()
        failed.startsSuccessfully = false
        let query = ItemQuery(query: failed)
        do {
            _ = try await query.searchMetadataItems(baseUrl: environment.root, onUpdate: nil)
            XCTFail("Expected start failure")
        } catch { XCTAssertTrue(failed.stopped) }
        let empty = FakeMetadataQuery()
        empty.initialEvents = [.gathered([])]
        let stream = ItemQuery(query: empty).downloadUbiqitousFiles(baseUrl: environment.root, fileManager: .default, config: .init())
        var count = 0
        for try await _ in stream { count += 1 }
        XCTAssertEqual(count, 0)
        XCTAssertTrue(empty.stopped)
    }

    func testFakeKeyValueStoreDoesNotUseSystemStore() async throws {
        let first = FakeUbiquitousKeyValueStore()
        let second = FakeUbiquitousKeyValueStore()
        first.set(Int64(42), forKey: "value")
        XCTAssertEqual(first.longLong(forKey: "value"), 42)
        XCTAssertNil(second.object(forKey: "value"))
        let adaptor = UbiquitousKeyValueStoreAdaptor(store: first)
        let key = CryoNamedKey(id: "message", for: String.self)
        try await adaptor.persist("hello", for: key)
        XCTAssertEqual(try adaptor.loadSynchronously(with: key), "hello")
        try await adaptor.remove(with: key)
        first.removeObject(forKey: "value")
        XCTAssertTrue(first.dictionaryRepresentation.isEmpty)
    }
}
