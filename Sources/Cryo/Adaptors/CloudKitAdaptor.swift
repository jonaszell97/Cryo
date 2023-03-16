
import CloudKit
import Foundation

internal protocol AnyCloudKitAdaptor: AnyObject, CryoDatabaseAdaptor {
    /// Delete a record with the given id.
    func delete(recordWithId id: CKRecord.ID) async throws
    
    /// Delete a table.
    func delete(tableName: String) async throws
    
    /// Save the given record.
    func save(record: CKRecord) async throws
    
    /// Fetch a record with the given id.
    func fetch(recordWithId id: CKRecord.ID) async throws -> CKRecord?
    
    /// Fetch a record with the given id.
    func fetchAll(tableName: String, predicate: NSPredicate, limit: Int) async throws -> [CKRecord]?
    
    /// Fetch a record with the given id.
    func fetchAllBatched(tableName: String, predicate: NSPredicate, receiveBatch: ([CKRecord]) throws -> Bool) async throws
    
}

// MARK: CryoDatabaseAdaptor implementation

extension AnyCloudKitAdaptor {
    public func select<Model: CryoModel>(from: Model.Type) async throws -> any CryoSelectQuery<Model> {
        fatalError("TODO")
    }
}

extension AnyCloudKitAdaptor {
    public func removeAll<Record: CryoModel>(of type: Record.Type) async throws {
        try await self.delete(tableName: Record.tableName)
    }
    
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws
        where Key.Value: CryoModel
    {
        let id = CKRecord.ID(recordName: key.id)
        guard let value else {
            try await self.delete(recordWithId: id)
            return
        }
        
        let modelType = Key.Value.self
        let record = CKRecord(recordType: modelType.tableName, recordID: id)
        let schema = await CryoSchemaManager.shared.schema(for: modelType)
        
        for columnDetails in schema {
            record[columnDetails.columnName] = try self.nsObject(from: columnDetails.getValue(value), valueType: columnDetails.type)
        }
        
        try await self.save(record: record)
    }
    
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value?
        where Key.Value: CryoModel
    {
        let id = CKRecord.ID(recordName: key.id)
        guard let record = try await self.fetch(recordWithId: id) else {
            return nil
        }
        
        let modelType = Key.Value.self
        let schema = await CryoSchemaManager.shared.schema(for: modelType)
        
        var data = [String: _AnyCryoColumnValue]()
        for columnDetails in schema {
            guard
                let object = record[columnDetails.columnName],
                let value = self.decodeValue(from: object, as: columnDetails.type)
            else {
                continue
            }
            
            data[columnDetails.columnName] = value
        }
        
        return try Key.Value(from: CryoModelDecoder(data: data))
    }
    
    public func loadAll<Record>(of type: Record.Type) async throws -> [Record]?
        where Record: CryoModel
    {
        var values = [Record]()
        try await self.loadAllBatched(of: type) { nextBatch in
            values.append(contentsOf: nextBatch)
            return true
        }
        
        return values
    }
    
    
    public func loadAll<Record>(of type: Record.Type, predicate: NSPredicate) async throws -> [Record]?
        where Record: CryoModel
    {
        var values = [Record]()
        try await self.loadAllBatched(of: type, predicate: predicate) { nextBatch in
            values.append(contentsOf: nextBatch)
            return true
        }
        
        return values
    }
    
    /// Load all values of the given `Record` type in batches. Not all adaptors support this operation.
    ///
    /// - Parameters:
    ///   - type: The record type of which all values should be loaded.
    ///   - receiveBatch: Closure that is invoked whenever a new batch of values is fetched. If this closure
    ///   returns `false`, no more batches will be fetched.
    /// - Returns: `true` if batched loading is supported.
    public func loadAllBatched<Record: CryoModel>(of type: Record.Type, receiveBatch: ([Record]) -> Bool) async throws {
        try await self._loadAllBatched(of: type,
                                       predicate: NSPredicate(value: true),
                                       receiveBatch: receiveBatch)
    }
    
    /// Load all values of the given `Key` type in batches. Not all adaptors support this operation.
    ///
    /// - Parameters:
    ///   - key: The Key type of which all values should be loaded.
    ///   - predicate: The predicate that loaded values must fulfill.
    ///   - receiveBatch: Closure that is invoked whenever a new batch of values is fetched. If this closure returns `false`, no more batches will be fetched.
    /// - Returns: `true` if batched loading is supported.
    public func loadAllBatched<Record: CryoModel>(of type: Record.Type,
                                                  predicate: NSPredicate,
                                                  receiveBatch: ([Record]) -> Bool) async throws {
        try await self._loadAllBatched(of: type, predicate: predicate,
                                       receiveBatch: receiveBatch)
    }
}

extension AnyCloudKitAdaptor {
    public func execute(operation: DatabaseOperation) async throws {
        try await ensureAvailability()
        
        switch operation.type {
        case .insert:
            fallthrough
        case .update:
            try await self.persist(key: operation.rowId, tableName: operation.tableName, data: operation.data)
        case .delete:
            if operation.tableName.isEmpty {
                try await self.removeAll()
                return
            }
            else if operation.rowId.isEmpty {
                try await self.delete(tableName: operation.tableName)
            }
            else {
                try await self.delete(recordWithId: .init(recordName: operation.rowId))
            }
        }
    }
}

// MARK: Utility functions

extension AnyCloudKitAdaptor {
    /// Persist a value from a database operation.
    func persist(key: String, tableName: String, data: [DatabaseOperationValue]) async throws {
        let id = CKRecord.ID(recordName: key)
        
        let record = CKRecord(recordType: tableName, recordID: id)
        for item in data {
            record[item.columnName] = item.value.objcValue
        }
        
        try await self.save(record: record)
    }
    
    /// Fetch all records of a given table that satisfy a predicate.
    func fetchAll(tableName: String, predicate: NSPredicate, limit: Int = 0) async throws -> [CKRecord]? {
        var records = [CKRecord]()
        try await self.fetchAllBatched(tableName: tableName, predicate: predicate) {
            records.append(contentsOf: $0)
            return limit == 0 || records.count < limit
        }
        
        return records
    }
    
    /// Load all values of the given Key type. Not all adaptors support this operation.
    func _loadAllBatched<Record: CryoModel>(of type: Record.Type,
                                            predicate: NSPredicate,
                                            receiveBatch: ([Record]) -> Bool) async throws {
        let schema = await CryoSchemaManager.shared.schema(for: Record.self)
        try await self.fetchAllBatched(tableName: Record.tableName, predicate: predicate) { records in
            var batch = [Record]()
            for record in records {
                var data = [String: _AnyCryoColumnValue]()
                for columnDetails in schema {
                    guard
                        let object = record[columnDetails.columnName],
                        let value = self.decodeValue(from: object, as: columnDetails.type)
                    else {
                        continue
                    }
                    
                    data[columnDetails.columnName] = value
                }
                
                let nextValue = try Record(from: CryoModelDecoder(data: data))
                batch.append(nextValue)
            }
            
            return receiveBatch(batch)
        }
    }
    
    /// Initialize from an NSObject representation.
    func decodeValue(from nsObject: __CKRecordObjCValue, as type: CryoColumnType) -> _AnyCryoColumnValue? {
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
    func nsObject(from value: _AnyCryoColumnValue, valueType: CryoColumnType) throws -> __CKRecordObjCValue {
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
        case let value as CryoColumnDateValue:
            return value.dateValue as NSDate
        case let value as CryoColumnDataValue:
            return try value.dataValue as NSData
            
        default:
            return (try JSONEncoder().encode(value)) as NSData
        }
    }
}

/// Implementation of ``CryoDatabaseAdaptor`` that persists values in a CloudKit database.
///
/// Values stored by this adaptor must conform to the ``CryoModel`` protocol. For every such type,
/// this adaptor creates a CloudKit table whose name is given by the ``CryoModel/tableName-3pg2z`` property.
///
/// A column is created for every model property that is annotated with either ``CryoColumn`` or ``CryoAsset``.
///
/// - Note: This adaptor does not support synchronous loading via ``CryoSyncronousAdaptor/loadSynchronously(with:)``.
///
/// Take the following model definition as an example:
///
/// ```swift
/// struct Message: CryoModel {
///     @CryoColumn var content: String
///     @CryoColumn var created: Date
///     @CryoAsset var attachment
/// }
///
/// try await adaptor.persist(Message(content: "Hello", created: Date.now, attachment: /*...*/),
///                           with: CryoNamedKey(id: "1", for: Message.self))
/// try await adaptor.persist(Message(content: "Hi", created: Date.now, attachment: /*...*/),
///                           with: CryoNamedKey(id: "2", for: Message.self))
/// try await adaptor.persist(Message(content: "How are you?", created: Date.now, attachment: /*...*/),
///                           with: CryoNamedKey(id: "3", for: Message.self))
///
/// ```
///
/// Based on this definition, `CloudKitAdaptor` will create a table in CloudKIt named `Message`
/// with the following structure:
///
/// | ID  | content: `NSString` | created: `NSDate` | attachment: `NSURL` |
/// | ---- | ---------- | ---------- | -------------- |
/// | 1   | "Hello"  | YYYY-MM-DD | /... |
/// | 2   | "Hi"  | YYYY-MM-DD | /... |
/// | 3   | "How are you?"  | YYYY-MM-DD | /... |
public final class CloudKitAdaptor {
    /// The configuration.
    let config: CryoConfig
    
    /// The iCloud container to store to.
    let container: CKContainer
    
    /// The database to store to.
    let database: CKDatabase
    
    /// The unique iCloud record ID for the user.
    var iCloudRecordID: String?
    
    /// Default initializer.
    public init(config: CryoConfig, containerIdentifier: String, database: KeyPath<CKContainer, CKDatabase>) async {
        self.config = config
        
        let container = CKContainer(identifier: containerIdentifier)
        self.container = container
        self.database = container[keyPath: database]
        
        self.iCloudRecordID = await withCheckedContinuation { continuation in
            container.fetchUserRecordID(completionHandler: { (recordID, error) in
                if let error {
                    config.log?(.fault, "error fetching user record id: \(error.localizedDescription)")
                }
                
                continuation.resume(returning: recordID?.recordName)
            })
        }
    }
}

// MARK: CryoDatabaseAdaptor implementation

extension CloudKitAdaptor {
    public func removeAll() async throws {
        for zone in try await database.allRecordZones() {
            try await database.deleteRecordZone(withID: zone.zoneID)
        }
    }

    /// Check for availability of the database.
    public func ensureAvailability() async throws {
        guard self.iCloudRecordID == nil else {
            return
        }
        
        self.iCloudRecordID = await withCheckedContinuation { continuation in
            container.fetchUserRecordID(completionHandler: { (recordID, error) in
                if let error {
                    self.config.log?(.fault, "error fetching user record id: \(error.localizedDescription)")
                }
                
                continuation.resume(returning: recordID?.recordName)
            })
        }
        
        guard self.iCloudRecordID == nil else {
            return
        }
        
        throw CryoError.backendNotAvailable
    }
    
    /// Whether CloudKit is available.
    public var isAvailable: Bool { iCloudRecordID != nil }
    
    public func observeAvailabilityChanges(_ callback: @escaping (Bool) -> Void) {
        // TODO: implement this
    }
}

// MARK: AnyCloudKitAdaptor implementation

extension CloudKitAdaptor: AnyCloudKitAdaptor {
    /// Delete a record with the given id.
    func delete(recordWithId id: CKRecord.ID) async throws {
        try await ensureAvailability()
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
    
    func delete(tableName: String) async throws {
        try await ensureAvailability()
        
        var recordIDs = [CKRecord.ID]()
        try await self.fetchAllBatched(tableName: tableName, predicate: NSPredicate(value: true)) { records in
            recordIDs.append(contentsOf: records.map { $0.recordID })
            return true
        }
        
        return try await withCheckedThrowingContinuation { continuation in
            database.modifyRecords(saving: [], deleting: recordIDs) { _ in
                continuation.resume()
            }
        }
    }
    
    /// Save the given record.
    func save(record: CKRecord) async throws {
        try await ensureAvailability()
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
        try await ensureAvailability()
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
    func fetchAllBatched(tableName: String, predicate: NSPredicate, receiveBatch: ([CKRecord]) throws -> Bool) async throws {
        try await ensureAvailability()
        
        let query = CKQuery(recordType: tableName, predicate: predicate)
        
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
}
