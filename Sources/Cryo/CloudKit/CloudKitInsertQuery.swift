
import CloudKit
import Foundation
import os

public final class CloudKitInsertQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedCloudKitInsertQuery
    
    /// Create an INSERT query.
    internal init(id: String, value: Model, replace: Bool, database: any CloudKitDatabase, config: CryoConfig?) throws {
        self.untypedQuery = try .init(id: id, value: value, replace: replace, database: database, config: config)
    }
}

extension CloudKitInsertQuery: CryoInsertQuery {
    public var id: String { untypedQuery.id }
    public var replace: Bool { untypedQuery.replace }
    public var value: Model { untypedQuery.value as! Model }
    
    public var queryString: String {
        untypedQuery.queryString
    }
    
    @discardableResult public func execute() async throws -> Bool {
        try await untypedQuery.execute()
    }
}

internal class UntypedCloudKitInsertQuery {
    private let schema: CryoSchema
    /// The ID of the record to insert.
    let id: String
    
    /// The model value to insert.
    let value: any CryoModel
    
    /// Whether to replace an existing value with the same key
    let replace: Bool
    
    /// The creation date of this query.
    let created: Date
    
    /// The database to store to.
    let database: any CloudKitDatabase
    
    let config: CryoConfig?
    
    /// Create a INSERT query.
    internal init(id: String, value: any CryoModel, replace: Bool, database: any CloudKitDatabase, config: CryoConfig?) throws {
        self.id = id
        self.schema = try CryoSchemaManager.shared.schema(for: type(of: value))
        self.value = value
        self.replace = replace
        self.created = config?.now() ?? Date()
        self.database = database
        
        self.config = config
    }
    
    /// The complete query string.
    public var queryString: String {
        let modelType = type(of: value)
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
            let results = try await database.modifyRecords(
                saving: [record], deleting: [],
                savePolicy: self.replace ? .changedKeys : .ifServerRecordUnchanged
            )
            if let error = results.saveResults.values.compactMap({ result -> Error? in
                guard case .failure(let error) = result,
                      CloudKitAdaptor.isRetryableCloudKitError(error) else { return nil }
                return error
            }).first {
                throw error
            }
            return results
        }
        
        for result in saveResults {
            switch result.value {
            case .success(let id):
                #if DEBUG
                config?.log?(.debug, "[CloudKitAdaptor] Success! \(id)")
                #endif
            case .failure(let err):
                if !replace, let cloudKitError = err as? CKError,
                   cloudKitError.code == .serverRecordChanged {
                    throw CryoError.duplicateId(id: self.id)
                }
                #if DEBUG
                config?.log?(.debug, "[CloudKitAdaptor] FAILURE: \(err.localizedDescription)")
                #endif
                throw err
            }
        }
        
        return true
    }
}
