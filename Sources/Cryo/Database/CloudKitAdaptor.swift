
import CloudKit
import Foundation

internal protocol AnyCloudKitAdaptor: AnyObject, CryoAdaptor {
    /// Delete a record with the given id.
    func delete(recordWithId id: CKRecord.ID) async throws
    
    /// Save the given record.
    func save(record: CKRecord) async throws
    
    /// Fetch a record with the given id.
    func fetch(recordWithId id: CKRecord.ID) async throws -> CKRecord?
    
    /// Fetch a record with the given id.
    func fetchAll(tableName: String, limit: Int) async throws -> [CKRecord]?
    
    /// Fetch a record with the given id.
    func fetchAllBatched(tableName: String, receiveBatch: ([CKRecord]) throws -> Bool) async throws
    
    /// Cache for schemas.
    var schemas: [String: CryoSchema] { get set }
}

extension AnyCloudKitAdaptor {
    /// Fetch a record with the given id.
    func fetchAll(tableName: String, limit: Int) async throws -> [CKRecord]? {
        var records = [CKRecord]()
        try await self.fetchAllBatched(tableName: tableName) {
            records.append(contentsOf: $0)
            return limit == 0 || records.count < limit
        }
        
        return records
    }
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
            let (valueType, extractValue) = details
            record[key] = try self.nsObject(from: extractValue(model), valueType: valueType)
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
        
        var data = [String: _AnyCryoColumnValue]()
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
    
    /// Load all values of the given Key type. Not all adaptors support this operation.
    public func loadAllBatched<Key: CryoKey>(with key: Key.Type, receiveBatch: ([Key.Value]) -> Bool) async throws -> Bool {
        guard let modelType = Key.Value.self as? CryoModel.Type else {
            return false
        }
        
        let schema = self.schema(for: modelType)
        try await self.fetchAllBatched(tableName: modelType.tableName) { records in
            var batch = [Key.Value]()
            for record in records {
                var data = [String: _AnyCryoColumnValue]()
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
                
                let nextValue = try Key.Value(from: CryoModelDecoder(data: data))
                batch.append(nextValue)
            }
            
            return receiveBatch(batch)
        }
        
        return true
    }
    
    /// Initialize from an NSObject representation.
    fileprivate func decodeValue(from nsObject: __CKRecordObjCValue, as type: CryoColumnType) -> _AnyCryoColumnValue? {
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
        case .asset:
            guard let value = nsObject as? CKAsset else { return nil }
            return value.fileURL
        case .data:
            guard let value = nsObject as? NSData else { return nil }
            return value as Data
        }
    }
    
    /// The NSObject representation oft his value.
    fileprivate func nsObject(from value: _AnyCryoColumnValue, valueType: CryoColumnType) throws -> __CKRecordObjCValue {
        switch value {
        case let url as URL:
            if case .asset = valueType {
                return CKAsset(fileURL: url)
            }
            
            return url.absoluteString as NSString
        case let value as CryoColumnIntValue:
            return value.integerValue as NSNumber
        case let value as CryoColumnDoubleValue:
            return value.doubleValue as NSNumber
        case let value as CryoColumnStringValue:
            return value.stringValue as NSString
        case let value as CryoColumnDataValue:
            return value.dataValue as NSData
        
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
    
    /// Fetch a record with the given id.
    func fetchAllBatched(tableName: String, receiveBatch: ([CKRecord]) throws -> Bool) async throws {
        let query = CKQuery(recordType: tableName, predicate: NSPredicate(value: true))
        
        var operation: CKQueryOperation? = CKQueryOperation(query: query)
        operation?.resultsLimit = CKQueryOperation.maximumResults
        
        while let nextOperation = operation {
            var data = [CKRecord]()
            var encounteredError = false
            
            let cursor: CKQueryOperation.Cursor? = try await withCheckedThrowingContinuation { continuation in
                nextOperation.queryResultBlock =  {
                    guard !encounteredError else { return }
                    continuation.resume(with: $0)
                }
                nextOperation.recordMatchedBlock = { _, result in
                    switch result {
                    case .success(let record):
                        data.append(record)
                    case .failure(let error):
                        encounteredError = true
                        continuation.resume(throwing: error)
                    }
                }
                
                self.database.add(nextOperation)
            }
            
            let shouldContinue = try receiveBatch(data)
            if let cursor = cursor, shouldContinue {
                operation = CKQueryOperation(cursor: cursor)
            }
            else {
                break
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
