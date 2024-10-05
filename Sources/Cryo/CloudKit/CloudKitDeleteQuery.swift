
import CloudKit
import Foundation

public final class CloudKitDeleteQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedCloudKitDeleteQuery
    
    /// Create an UPDATE query.
    internal init(from: Model.Type, id: String?, database: CKDatabase, config: CryoConfig?) throws {
        self.untypedQuery = try .init(for: Model.self, id: id, database: database, config: config)
    }
    
    /// The database operation for this query.
    var operation: DatabaseOperation {
        get async throws {
            .delete(date: .now, tableName: Model.tableName, rowId: untypedQuery.id, whereClauses: untypedQuery.whereClauses)
        }
    }
}

extension CloudKitDeleteQuery: CryoDeleteQuery {
    public var id: String? { untypedQuery.id }
    public var whereClauses: [CryoQueryWhereClause] { untypedQuery.whereClauses }
    
    public var queryString: String {
        untypedQuery.queryString
    }
    
    @discardableResult public func execute() async throws -> Int {
        try await untypedQuery.execute()
    }
    
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) throws -> Self {
        _ = try untypedQuery.where(columnName, operation: operation, value: value)
        return self
    }
}

internal class UntypedCloudKitDeleteQuery {
    /// The ID of the row to delete.
    let id: String?
    
    /// The model type.
    let modelType: any CryoModel.Type
    
    /// The where clauses.
    var whereClauses: [CryoQueryWhereClause]
    
    /// The database to store to.
    let database: CKDatabase
    
    #if DEBUG
    let config: CryoConfig?
    #endif
    
    /// Create a DELETE query.
    internal init(for modelType: any CryoModel.Type, id: String?, database: CKDatabase, config: CryoConfig?) throws {
        self.id = id
        self.modelType = modelType
        self.database = database
        self.whereClauses = []
        
        #if DEBUG
        self.config = config
        #endif
    }
    
    /// The complete query string.
    public var queryString: String {
        var result = "DELETE FROM \(modelType.tableName)"
        for i in 0..<whereClauses.count {
            if i == 0 {
                result += " WHERE "
            }
            else {
                result += " AND "
            }
            
            let clause = whereClauses[i]
            result += "\(clause.columnName) \(CloudKitAdaptor.formatOperator(clause.operation)) \(CloudKitAdaptor.placeholderSymbol(for: clause.value))"
        }
        
        return result
    }
}

extension UntypedCloudKitDeleteQuery {
    @discardableResult public func execute() async throws -> Int {
        let records = try await UntypedCloudKitSelectQuery.fetch(id: id, modelType: modelType,
                                                                 whereClauses: whereClauses,
                                                                 resultsLimit: nil,
                                                                 sortingClauses: [],
                                                                 database: database)
        
        #if DEBUG
        config?.log?(.debug, "[CloudKitAdaptor] \(queryString), WHERE \(whereClauses.map { "\($0.value)" })")
        #endif
        
        _ = try await CloudKitAdaptor.cloudKitOperation {
            try await database.modifyRecords(saving: [], deleting: records.map { $0.recordID })
        }
        
        return records.count
    }
    
    /// Attach a WHERE clause to this query.
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) throws -> Self {
        self.whereClauses.append(.init(columnName: columnName,
                                       operation: operation,
                                       value: try .init(value: value)))
        
        return self
    }
}
