
import CloudKit
import Foundation

public final class CloudKitAdaptor {
    /// The configuration.
    let config: CryoConfig
    
    /// The iCloud container to store to.
    public let container: CKContainer
    
    /// The database to store to.
    public let database: CKDatabase
    
    /// The unique iCloud record ID for the user.
    public let iCloudRecordID: String
    
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

fileprivate extension CloudKitAdaptor {
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

extension CloudKitAdaptor: CryoDatabaseAdaptor {
    /// Persist the given value for a key.
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws where Key.Value: CryoModel {
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
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value? where Key.Value: CryoModel {
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
    
    public func removeAll() async throws {
        for zone in try await database.allRecordZones() {
            try await database.deleteRecordZone(withID: zone.zoneID)
        }
    }
}

internal extension CryoValue {
    /// The NSObject representation oft his value.
    var nsObject: __CKRecordObjCValue {
        switch self {
        case .integer(let value):
            return value as NSNumber
        case .double(let value):
            return value as NSNumber
        case .text(let value):
            return value as NSString
        case .date(let value):
            return value as NSDate
        case .bool(let value):
            return value as NSNumber
        case .data(let value):
            return value as NSData
        }
    }
    
    /// Initialize from an NSObject representation.
    init?(from nsObject: __CKRecordObjCValue, as type: ValueType) {
        switch type {
        case .integer:
            guard let value = nsObject as? NSNumber else { return nil }
            self = .integer(value: Int(truncating: value))
        case .double:
            guard let value = nsObject as? NSNumber else { return nil }
            self = .double(value: Double(truncating: value))
        case .text:
            guard let value = nsObject as? NSString else { return nil }
            self = .text(value: value as String)
        case .date:
            guard let value = nsObject as? NSDate else { return nil }
            self = .date(value: Date(timeIntervalSinceReferenceDate: value.timeIntervalSinceReferenceDate))
        case .bool:
            guard let value = nsObject as? NSNumber else { return nil }
            self = .bool(value: value != 0)
        case .data:
            guard let value = nsObject as? NSData else { return nil }
            self = .data(value: value as Data)
        }
    }
}
