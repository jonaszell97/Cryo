
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
    
    /// Cache of schema data.
    var schemas: [String: CryoSchema] = [:]
    
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
    
    /// Find or create a schema.
    func schema<Key: CryoKey>(for key: Key.Type) -> CryoSchema where Key.Value: CryoModel {
        let schemaName = "\(Key.Value.self)"
        if let schema = self.schemas[schemaName] {
            return schema
        }
        
        let schema = Key.Value.schema
        self.schemas[schemaName] = schema
        
        return schema
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
        let schema = self.schema(for: Key.self)
        
        for (key, details) in schema {
            let (_, extractValue) = details
            record[key] = extractValue(value).nsObject
        }
        
        try await self.save(record: record)
    }
    
    /// - returns: The value previously persisted for `key`, or nil if none exists.
    func load<Key: CryoKey>(with key: Key) async throws -> Key.Value? where Key.Value: CryoModel {
        let id = CKRecord.ID(recordName: key.id)
        guard let record = try await self.fetch(recordWithId: id) else {
            return nil
        }
        
        let schema = self.schema(for: Key.self)
        
        var data = [String: CryoValue]()
        for (key, details) in schema {
            let (valueType, _) = details
            guard
                let object = record[key],
                let value = CryoValue(from: object, as: valueType)
            else {
                continue
            }
            
            data[key] = value
        }
        
        return try Key.Value(from: CryoModelDecoder(data: data))
    }
    
    func removeAll() async throws {
        database.removeAll()
    }
}
