
import CloudKit
import Foundation

internal protocol AnyCloudKitAdaptor: AnyObject, CryoAdaptor {
    /// Delete a record with the given id.
    func delete(recordWithId id: CKRecord.ID) async throws
    
    /// Save the given record.
    func save(record: CKRecord) async throws
    
    /// Fetch a record with the given id.
    func fetch(recordWithId id: CKRecord.ID) async throws -> CKRecord?
    
    /// Cache for schemas.
    var schemas: [String: CryoSchema] { get set }
}

extension AnyCloudKitAdaptor {
    /// Find or create a schema.
    func schema<Model: CryoModel>(for model: Model.Type) -> CryoSchema {
        let schemaName = "\(Model.self)"
        if let schema = self.schemas[schemaName] {
            return schema
        }
        
        let schema = Model.schema
        self.schemas[schemaName] = schema
        
        return schema
    }
    
    /// Persist the given value for a key.
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws {
        let id = CKRecord.ID(recordName: key.id)
        guard let value else {
            try await self.delete(recordWithId: id)
            return
        }
        
        guard let model = value as? CryoModel else {
            throw CryoError.cannotPersistValue(valueType: Key.Value.self, adaptorType: CloudKitAdaptor.self)
        }
        
        let modelType = type(of: model)
        let record = CKRecord(recordType: modelType.tableName, recordID: id)
        let schema = self.schema(for: modelType)
        
        for (key, details) in schema {
            let (_, extractValue) = details
            record[key] = try self.nsObject(from: extractValue(model))
        }
        
        try await self.save(record: record)
    }
    
    /// - returns: The value previously persisted for `key`, or nil if none exists.
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value? {
        let id = CKRecord.ID(recordName: key.id)
        guard let record = try await self.fetch(recordWithId: id) else {
            return nil
        }
        
        guard let modelType = Key.Value.self as? CryoModel.Type else {
            throw CryoError.cannotPersistValue(valueType: Key.Value.self, adaptorType: CloudKitAdaptor.self)
        }
        
        let schema = self.schema(for: modelType)
        
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
}

public final class CloudKitAdaptor {
    /// The configuration.
    let config: CryoConfig
    
    /// The iCloud container to store to.
    let container: CKContainer
    
    /// The database to store to.
    let database: CKDatabase
    
    /// The unique iCloud record ID for the user.
    let iCloudRecordID: String
    
    /// Cache of schema data.
    var schemas: [String: CryoSchema] = [:]
    
    /// Default initializer.
    public init?(config: CryoConfig, containerIdentifier: String, database: KeyPath<CKContainer, CKDatabase>) async {
        self.config = config
        
        let container = CKContainer(identifier: containerIdentifier)
        self.container = container
        self.database = container[keyPath: database]
        
        let iCloudRecordID: String? = await withCheckedContinuation { continuation in
            container.fetchUserRecordID(completionHandler: { (recordID, error) in
                if let error {
                    config.log?(.fault, "error fetching user record id: \(error.localizedDescription)")
                }
                
                continuation.resume(returning: recordID?.recordName)
            })
        }
        
        guard let iCloudRecordID else {
            return nil
        }
        
        self.iCloudRecordID = iCloudRecordID
    }
}

extension CloudKitAdaptor: AnyCloudKitAdaptor {
    /// Delete a record with the given id.
    func delete(recordWithId id: CKRecord.ID) async throws {
        return try await withCheckedThrowingContinuation { continuation in
            database.delete(withRecordID: id) { _, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                
                continuation.resume()
            }
        }
    }
    
    /// Save the given record.
    func save(record: CKRecord) async throws {
        return try await withCheckedThrowingContinuation { continuation in
            database.save(record) { _, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                
                continuation.resume()
            }
        }
    }
    
    /// Fetch a record with the given id.
    func fetch(recordWithId id: CKRecord.ID) async throws -> CKRecord? {
        return try await withCheckedThrowingContinuation { continuation in
            database.fetch(withRecordID: id) { record, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                
                continuation.resume(returning: record)
            }
        }
    }
    
    /// Find or create a schema.
    func schema<Model: CryoModel>(for model: Model.Type) -> CryoSchema {
        let schemaName = "\(Model.self)"
        if let schema = self.schemas[schemaName] {
            return schema
        }
        
        let schema = Model.schema
        self.schemas[schemaName] = schema
        
        return schema
    }
     
    public func removeAll() async throws {
        for zone in try await database.allRecordZones() {
            try await database.deleteRecordZone(withID: zone.zoneID)
        }
    }
}
