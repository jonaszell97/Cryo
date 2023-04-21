
import CloudKit
import Foundation

public final class CloudKitInsertQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedCloudKitInsertQuery
    
    /// Create an INSERT query.
    internal init(id: String, value: Model, replace: Bool, database: CKDatabase, config: CryoConfig?) throws {
        self.untypedQuery = try .init(id: id, value: value, replace: replace, database: database, config: config)
    }
}

extension CloudKitInsertQuery: CryoInsertQuery {
    public var id: String { untypedQuery.id }
    public var value: Model { untypedQuery.value as! Model }
    
    public var queryString: String {
        untypedQuery.queryString
    }
    
    @discardableResult public func execute() async throws -> Bool {
        try await untypedQuery.execute()
    }
}

internal class UntypedCloudKitInsertQuery {
    /// The ID of the record to insert.
    let id: String
    
    /// The model value to insert.
    let value: any CryoModel
    
    /// Whether to replace an existing value with the same key
    let replace: Bool
    
    /// The creation date of this query.
    let created: Date
    
    /// The database to store to.
    let database: CKDatabase
    
    #if DEBUG
    let config: CryoConfig?
    #endif
    
    /// Create a INSERT query.
    internal init(id: String, value: any CryoModel, replace: Bool, database: CKDatabase, config: CryoConfig?) throws {
        self.id = id
        self.value = value
        self.replace = replace
        self.created = .now
        self.database = database
        
        #if DEBUG
        self.config = config
        #endif
    }
    
    /// The complete query string.
    public var queryString: String {
        let modelType = type(of: value)
        let schema = CryoSchemaManager.shared.schema(for: modelType)
        let columns: [String] = schema.columns.map { $0.columnName }
        
        let result = """
INSERT \(replace ? "OR REPLACE " : "")INTO \(modelType.tableName)(\(columns.joined(separator: ",")))
    VALUES (\(columns.map { _ in "?" }.joined(separator: ",")));
"""
        
        return result
    }
}

extension UntypedCloudKitInsertQuery {
    @discardableResult public func execute() async throws -> Bool {
        let modelType = type(of: value)
        let record = CKRecord(recordType: modelType.tableName, recordID: CKRecord.ID(recordName: id))
        let schema = CryoSchemaManager.shared.schema(for: modelType)
        
        for columnDetails in schema.columns {
            record[columnDetails.columnName] = try CloudKitAdaptor.nsObject(from: columnDetails.getValue(value),
                                                                            column: columnDetails)
        }
        
        return try await withCheckedThrowingContinuation { continuation in
            let saveRecordsOperation = CKModifyRecordsOperation()
            saveRecordsOperation.recordsToSave = [record]
            
            if replace {
                saveRecordsOperation.savePolicy = .allKeys
            }
            else {
                saveRecordsOperation.savePolicy = .ifServerRecordUnchanged
            }
            
            saveRecordsOperation.completionBlock = {
                continuation.resume(returning: true)
            }
            
            database.add(saveRecordsOperation)
        }
    }
}
