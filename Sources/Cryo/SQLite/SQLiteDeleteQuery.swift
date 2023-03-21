
import Foundation
import SQLite3

public final class SQLiteDeleteQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedSQLiteDeleteQuery
    
    /// Create an UPDATE query.
    internal init(id: String?, connection: OpaquePointer, config: CryoConfig?) throws {
        self.untypedQuery = try .init(id: id, modelType: Model.self, connection: connection, config: config)
    }
}

extension SQLiteDeleteQuery: CryoDeleteQuery {
    public var id: String? { untypedQuery.id }
    public var whereClauses: [CryoQueryWhereClause] { untypedQuery.whereClauses }
    
    public var queryString: String {
        get async {
            await untypedQuery.queryString
        }
    }
    
    @discardableResult public func execute() async throws -> Int {
        try await untypedQuery.execute()
    }
    
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) async throws -> Self {
        _ = try await untypedQuery.where(columnName, operation: operation, value: value)
        return self
    }
}

internal class UntypedSQLiteDeleteQuery {
    /// The ID of the row to delete.
    let id: String?
    
    /// The model type.
    let modelType: any CryoModel.Type
    
    /// The where clauses.
    var whereClauses: [CryoQueryWhereClause]
    
    /// The compiled query statement.
    var queryStatement: OpaquePointer? = nil
    
    /// The complete query string.
    var completeQueryString: String? = nil
    
    /// The SQLite connection.
    let connection: OpaquePointer
    
    #if DEBUG
    let config: CryoConfig?
    #endif
    
    /// Create a SELECT query.
    internal init(id: String?, modelType: any CryoModel.Type, connection: OpaquePointer, config: CryoConfig?) throws {
        self.connection = connection
        self.modelType = modelType
        self.id = id
        self.whereClauses = []
        
        #if DEBUG
        self.config = config
        #endif
    }
    
    /// The complete query string.
    public var queryString: String {
        get async {
            if let completeQueryString {
                return completeQueryString
            }
            
            var result = "DELETE FROM \(modelType.tableName)"
            let hasId = id != nil
            
            if hasId || !whereClauses.isEmpty {
                result += " WHERE "
            }
            
            if hasId {
                result += "_cryo_key = ?"
            }
            
            for i in 0..<whereClauses.count {
                if i > 0 || hasId {
                    result += " AND "
                }
                
                result += "\(whereClauses[i].columnName) \(SQLiteAdaptor.formatOperator(whereClauses[i].operation)) ?"
            }
            
            self.completeQueryString = result
            return result
        }
    }
}

extension UntypedSQLiteDeleteQuery {
    /// Get the compiled query statement.
    func compiledQuery() async throws -> OpaquePointer {
        if let queryStatement {
            return queryStatement
        }
        
        let queryString = await self.queryString
        var queryStatement: OpaquePointer?
        
        let prepareStatus = sqlite3_prepare_v3(connection, queryString, -1, 0, &queryStatement, nil)
        guard prepareStatus == SQLITE_OK, let queryStatement else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryCompilationFailed(query: queryString, status: prepareStatus, message: message)
        }
        
        var indexOffset = 0
        if let id {
            SQLiteAdaptor.bind(queryStatement, value: .string(value: id), index: Int32(1))
            indexOffset += 1
        }
        
        for i in 0..<whereClauses.count {
            SQLiteAdaptor.bind(queryStatement, value: whereClauses[i].value, index: Int32(i + 1 + indexOffset))
        }
        
        self.queryStatement = queryStatement
        return queryStatement
    }
}

extension UntypedSQLiteDeleteQuery {
    @discardableResult public func execute() async throws -> Int {
        let queryStatement = try await self.compiledQuery()
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        #if DEBUG
        config?.log?(.debug, "[SQLite3Connection] query \(await queryString), bindings \(whereClauses.map { "\($0.value)" })")
        #endif
        
        let executeStatus = sqlite3_step(queryStatement)
        guard executeStatus == SQLITE_DONE else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryExecutionFailed(query: await queryString,
                                                 status: executeStatus,
                                                 message: message)
        }
        
        return Int(sqlite3_changes(connection))
    }
    
    /// Attach a WHERE clause to this query.
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) async throws -> Self {
        guard self.queryStatement == nil else {
            throw CryoError.modifyingFinalizedQuery
        }
        
        self.whereClauses.append(.init(columnName: columnName,
                                       operation: operation,
                                       value: try .init(value: value)))
        return self
    }
}
