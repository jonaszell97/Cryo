# Cryo

Cryo is a persistence library for Swift apps using Swift Concurrency. It provides a unified API for `UserDefaults`, `NSUbiquitousKeyValueStore`, local and iCloud document storage, as well as CloudKit.

## Installation

Cryo can be added as a dependency in your project using Swift Package Manager.

```swift
// ...
dependencies: [
    .package(url: "https://github.com/jonaszell97/Cryo.git", from: "0.1.0"),
],
// ...
```

## Documentation

You can find the documentation for this package [here](https://cryo.jonaszell.dev).

## Running tests

Run these commands from the standalone Cryo repository:

```sh
make test-library-cryo-fast                         # macOS subset
make test-library-cryo-fast FILTER=InjectionSeamTests
make test-library-cryo SIMULATOR="iPhone 16 Pro"      # full iOS simulator suite
make test-library-cryo SANITIZER=Address
make test-library-cryo SANITIZER=Thread
make test-all                                      # full simulator suite
```

The simulator target uses the shared `Cryo` package scheme and compiles the
UIKit-gated `CloudSyncable` implementation. Choose an installed simulator with
`SIMULATOR` and `SIMULATOR_OS`, or pass a complete Xcode destination with
`DESTINATION`. For example, use `SIMULATOR_OS=26.0` when the named device is on
that runtime rather than the newest installed runtime. Set
`DEVELOPER_DIR` to select an Xcode installation. `DERIVED_DATA_PATH`,
`TEST_OPTIONS`, and `RESULT_BUNDLE_PATH` are also configurable; each result bundle
path must be new. The default options include `-skipMacroValidation` for Toolbox.
CI runs the simulator suite normally and with Address Sanitizer, using the
revisions in `Package.resolved`.

Tests use per-test temporary directories and UserDefaults suites, a manual clock,
and an in-memory CloudKit transport. The transport exercises Cryo's production
queries without an iCloud account. Metadata queries and ubiquitous key-value
storage also have test fakes. This infrastructure does not emulate Apple's
server or fix the later-phase behavior issues recorded in `003_cryo-audit.md`.
