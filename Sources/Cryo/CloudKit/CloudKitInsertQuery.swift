
import CloudKit
import Foundation
import os

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
        
        var log: Optional<(OSLogType, String) -> Void> = nil
        
        #if DEBUG
        log = config?.log
        var valuesStr = ""
        for columnDetails in schema.columns {
            valuesStr += "\(columnDetails.columnName): \(String(describing: record[columnDetails.columnName]).prefix(50)) "
        }
        
        config?.log?(.debug, "[CloudKitAdaptor] \(queryString); \(valuesStr)")
        #endif
        
        let (saveResults, _) = try await CloudKitAdaptor.cloudKitOperation(log: log) {
            try await database.modifyRecords(
                saving: [record], deleting: [],
                savePolicy: self.replace ? .changedKeys : .ifServerRecordUnchanged
            )
        }
        
        #if DEBUG
        for result in saveResults {
            switch result.value {
            case .success(let id):
                config?.log?(.debug, "[CloudKitAdaptor] Success! \(id)")
            case .failure(let err):
                config?.log?(.debug, "[CloudKitAdaptor] FAILURE: \(err.localizedDescription)")
            }
        }
        #else
        _ = saveResults
        #endif
        
        return true
    }
}
