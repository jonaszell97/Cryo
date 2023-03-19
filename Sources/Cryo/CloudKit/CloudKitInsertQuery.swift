
import CloudKit
import Foundation

public final class CloudKitInsertQuery<Model: CryoModel> {
    /// The ID of the record to insert.
    let id: String
    
    /// The model value to insert.
    let value: Model
    
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
    internal init(id: String, value: Model, replace: Bool, database: CKDatabase, config: CryoConfig?) throws {
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
        get async {
            let schema = await CryoSchemaManager.shared.schema(for: Model.self)
            let columns: [String] = schema.columns.map { $0.columnName }
            
            let result = """
INSERT \(replace ? "OR REPLACE " : "")INTO \(Model.tableName)(\(columns.joined(separator: ",")))
    VALUES (\(columns.map { _ in "?" }.joined(separator: ",")));
"""
            
            return result
        }
    }
}

extension CloudKitInsertQuery: CryoInsertQuery {
    @discardableResult public func execute() async throws -> Bool {
        let modelType = Model.self
        let record = CKRecord(recordType: modelType.tableName, recordID: CKRecord.ID(recordName: id))
        let schema = await CryoSchemaManager.shared.schema(for: modelType)
        
        for columnDetails in schema.columns {
            record[columnDetails.columnName] = try CloudKitAdaptor.nsObject(from: columnDetails.getValue(value),
                                                                            valueType: columnDetails.type)
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
