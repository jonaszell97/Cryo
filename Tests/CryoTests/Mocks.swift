
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
            record[key] = try self.nsObject(from: extractValue(value))
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
        
        var data = [String: any CryoDatabaseValue]()
        for (key, details) in schema {
            let (valueType, _) = details
            guard
                let object = record[key],
                let value = self.decodeValue(from: object, as: valueType)
            else {
                continue
            }
            
            data[key] = value
        }
        
        return try Key.Value(from: CryoModelDecoder(data: data))
    }
    
    /// Initialize from an NSObject representation.
    fileprivate func decodeValue(from nsObject: __CKRecordObjCValue, as type: CryoColumnType) -> (any CryoDatabaseValue)? {
        switch type {
        case .integer:
            guard let value = nsObject as? NSNumber else { return nil }
            return Int(truncating: value)
        case .double:
            guard let value = nsObject as? NSNumber else { return nil }
            return Double(truncating: value)
        case .text:
            guard let value = nsObject as? NSString else { return nil }
            return value as String
        case .date:
            guard let value = nsObject as? NSDate else { return nil }
            return Date(timeIntervalSinceReferenceDate: value.timeIntervalSinceReferenceDate)
        case .bool:
            guard let value = nsObject as? NSNumber else { return nil }
            return value != 0
        case .data:
            guard let value = nsObject as? NSData else { return nil }
            return value as Data
        }
    }
    
    /// The NSObject representation oft his value.
    fileprivate func nsObject(from value: any CryoDatabaseValue) throws -> __CKRecordObjCValue {
        switch value {
        case is Int:
            fallthrough
        case is Float:
            fallthrough
        case is Double:
            return value as! NSNumber
        case let value as String:
            return value as NSString
        case let value as Date:
            return value as NSDate
        case let value as Bool:
            return value as NSNumber
        case let value as Data:
            return value as NSData
        default:
            return (try JSONEncoder().encode(value)) as NSData
        }
    }
    
    func removeAll() async throws {
        database.removeAll()
    }
}
