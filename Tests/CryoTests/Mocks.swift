
import CloudKit
@testable import Cryo

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

extension MockCloudKitAdaptor: AnyCloudKitAdaptor {
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
    
    func removeAll() async throws {
        database.removeAll()
    }
}
