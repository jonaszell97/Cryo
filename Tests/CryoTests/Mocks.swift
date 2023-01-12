
import CloudKit
@testable import Cryo

struct TestModel: CryoModel {
    static var tableName: String { "TestModel" }
    static var model: CryoModelDetails<Self> = [
        "x": (.integer, \.$x),
        "y": (.text, \.$y),
        "z": (.data, \.$z),
    ]
    
    @CryoColumn var x: Int
    @CryoColumn var y: String
    @CryoColumn var z: Data
    
    init(x: Int, y: String, z: Data) {
        self._x = .init(wrappedValue: x)
        self._y = .init(wrappedValue: y)
        self._z = .init(wrappedValue: z)
    }
    
    init() {
        self.x = 0
        self.y = ""
        self.z = .init()
    }
}

extension TestModel: Equatable {
    static func ==(lhs: TestModel, rhs: TestModel) -> Bool {
        lhs.x == rhs.x && lhs.y == rhs.y && lhs.z == rhs.z
    }
}

final class MockCloudKitAdaptor {
    /// The database to store to.
    var database: [CKRecord.ID: CKRecord]
    
    /// Default initializer.
    init() {
        self.database = [:]
    }
}

fileprivate extension MockCloudKitAdaptor {
    /// Delete a record with the given id.
    func delete(recordWithId id: CKRecord.ID) async throws {
        database[id] = nil
    }
    
    /// Save the given record.
    func save(record: CKRecord) async throws {
        database[record.recordID] = record
    }
    
    /// Fetch a record with the given id.
    func fetch(recordWithId id: CKRecord.ID) async throws -> CKRecord? {
        database[id]
    }
}

extension MockCloudKitAdaptor: CryoDatabaseAdaptor {
    /// Persist the given value for a key.
    func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws where Key.Value: CryoModel {
        let id = CKRecord.ID(recordName: key.id)
        guard let value else {
            try await self.delete(recordWithId: id)
            
            return
        }
        
        let record = CKRecord(recordType: Key.Value.tableName, recordID: id)
        for (key, value) in value.data {
            record[key] = value.nsObject
        }
        
        try await self.save(record: record)
    }
    
    /// - returns: The value previously persisted for `key`, or nil if none exists.
    func load<Key: CryoKey>(with key: Key) async throws -> Key.Value? where Key.Value: CryoModel {
        let id = CKRecord.ID(recordName: key.id)
        guard let record = try await self.fetch(recordWithId: id) else {
            return nil
        }
        
        var instance = Key.Value()
        for (key, details) in Key.Value.model {
            guard
                let object = record[key],
                let value = CryoValue(from: object, as: details.0)
            else {
                continue
            }
            
            instance[keyPath: details.1] = value
        }
        
        return instance
    }
    
    func removeAll() async throws {
        database.removeAll()
    }
}
