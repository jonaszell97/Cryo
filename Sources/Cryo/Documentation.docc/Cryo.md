# ``Cryo``

Cryo is a persistence library for Swift apps using Swift Concurrency. It provides a unified API for `UserDefaults`, `NSUbiquitousKeyValueStore`, local and iCloud document storage, as well as CloudKit.

## Overview

Persistence is handled by implementations of ``CryoAdaptor``. Cryo provides persistence backends using `UserDefaults` (``UserDefaultsAdaptor``), `NSUbiquitousKeyValueStore` (``UbiquitousKeyValueStoreAdaptor``), documents (``DocumentAdaptor``), as well as `CloudKit` (``CloudKitAdaptor``).

To persist a resource, you simply create a ``CryoKey`` that uniquely identifies it and call the ``CryoAdaptor/persist(_:for:)`` method on an adaptor. The resource can then be retrieved using one of the ``CryoAdaptor/load(with:)-4uswy``, ``CryoAdaptor/loadSynchronously(with:)-6ganv``, ``CryoAdaptor/loadAll(with:)-27f70``, or ``CryoAdaptor/loadAllBatched(with:receiveBatch:)-4sv4g`` methods.

```swift
// Persist data using UserDefaults
let adaptor = UserDefaultsAdaptor.shared
try await adaptor.set(420, CryoNamedKey(id: "myInteger", for: Int.self))
try await adaptor.set("Hello, World", CryoNamedKey(id: "myString", for: String.self))

print(try await adaptor.load(with: CryoNamedKey(id: "myInteger", for: Int.self))) // prints "420"
print(try await adaptor.load(with: CryoNamedKey(id: "myString", for: String.self))) // prints "Hello, World"
```

```swift
// Write data to a local document
struct BigData: Codable {
    let values: [Double]
}

struct BigDataKey: CryoKey {
    typealias Value = BigData
    var id: String
}

let data: BigData = /* ... */
let key = BigDataKey(id: "myData")

// Saves to the file '<AppDocuments>/.cryo/myData'
try await DocumentAdaptor.sharedLocal.persist(bigData, key)

// Later ...
_ = try await DocumentAdaptor.sharedLocal.load(with: key)
```

```swift
// Save data in CloudKit
struct Message: CryoModel {
    @CryoColumn var content: String
    @CryoColumn var created: Date
    @CryoAsset var attachment
}

try await adaptor.persist(Message(content: "Hello", created: Date.now, attachment: /*...*/),
                          with: CryoNamedKey(id: "1", for: Message.self))
try await adaptor.persist(Message(content: "Hi", created: Date.now, attachment: /*...*/),
                          with: CryoNamedKey(id: "2", for: Message.self))
try await adaptor.persist(Message(content: "How are you?", created: Date.now, attachment: /*...*/),
                          with: CryoNamedKey(id: "3", for: Message.self))
```

## Topics

### Models

- ``CryoKey``
- ``CryoNamedKey``
- ``CryoConfig``
- ``CryoError``

### Adaptors

- ``CryoAdaptor``
- ``UserDefaultsAdaptor``
- ``UbiquitousKeyValueStoreAdaptor``
- ``DocumentAdaptor``

### Database Persistence

- ``CloudKitAdaptor``
- ``CryoModel``
- ``CryoColumn``
- ``CryoAsset``
- ``CryoColumnIntValue``
- ``CryoColumnDoubleValue``
- ``CryoColumnStringValue``
- ``CryoColumnDateValue``
- ``CryoColumnDataValue``

### Property Wrappers

- ``CryoPersisted``
- ``CryoKeyValue``
- ``CryoUbiquitousKeyValue``
- ``CryoLocalDocument``
