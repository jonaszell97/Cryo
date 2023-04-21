
import Foundation
import SQLite3

public final class SQLiteUpdateQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedSQLiteUpdateQuery
    
    /// Create an UPDATE query.
    internal init(from: Model.Type, id: String?, connection: OpaquePointer, config: CryoConfig?) throws {
        self.untypedQuery = try .init(id: id, modelType: Model.self, connection: connection, config: config)
    }
}

extension SQLiteUpdateQuery: CryoUpdateQuery {
    public var id: String? { untypedQuery.id }
    public var whereClauses: [CryoQueryWhereClause] { untypedQuery.whereClauses }
    public var setClauses: [CryoQuerySetClause] { untypedQuery.setClauses }
    
    public var queryString: String {
        untypedQuery.queryString
    }
    
    @discardableResult public func execute() throws -> Int {
        try untypedQuery.execute()
    }
    
    
    public func set<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        to value: Value
    ) throws -> Self {
        _ = try untypedQuery.set(columnName, to: value)
        return self
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

internal class UntypedSQLiteUpdateQuery {
    /// The ID of the row to update.
    let id: String?
    
    /// The model type.
    let modelType: any CryoModel.Type
    
    /// The set clauses.
    var setClauses: [CryoQuerySetClause]
    
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
        self.id = id
        self.modelType = modelType
        self.setClauses = []
        self.whereClauses = []
        
        #if DEBUG
        self.config = config
        #endif
    }
    
    /// The complete query string.
    public var queryString: String {
        if let completeQueryString {
            return completeQueryString
        }
        
        let hasId = id != nil
        var result = "UPDATE \(modelType.tableName) SET _cryo_modified = ?"
        
        // Set clauses
        
        for i in 0..<setClauses.count {
            result += ", \(setClauses[i].columnName) = ?"
        }
        
        // Where clauses
        
        if hasId || !whereClauses.isEmpty {
            result += " WHERE "
        }
        
        if hasId {
            result += "id == ?"
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

extension UntypedSQLiteUpdateQuery {
    /// Get the compiled query statement.
    func compiledQuery() throws -> OpaquePointer {
        if let queryStatement {
            return queryStatement
        }
        
        let queryString = self.queryString
        var queryStatement: OpaquePointer?
        
        let prepareStatus = sqlite3_prepare_v3(connection, queryString, -1, 0, &queryStatement, nil)
        guard prepareStatus == SQLITE_OK, let queryStatement else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryCompilationFailed(query: queryString, status: prepareStatus, message: message)
        }
        
        var bindIndex = 1
        
        SQLiteAdaptor.bind(queryStatement, value: .date(value: .now), index: Int32(bindIndex))
        bindIndex += 1
        
        for i in 0..<setClauses.count {
            SQLiteAdaptor.bind(queryStatement, value: setClauses[i].value, index: Int32(bindIndex))
            bindIndex += 1
        }
        
        if let id {
            SQLiteAdaptor.bind(queryStatement, value: .string(value: id), index: Int32(bindIndex))
            bindIndex += 1
        }
        
        for i in 0..<whereClauses.count {
            SQLiteAdaptor.bind(queryStatement, value: whereClauses[i].value, index: Int32(bindIndex))
            bindIndex += 1
        }
        
        self.queryStatement = queryStatement
        return queryStatement
    }
}

extension UntypedSQLiteUpdateQuery {
    @discardableResult public func execute() throws -> Int {
        let queryStatement = try self.compiledQuery()
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        #if DEBUG
        config?.log?(.debug, "[SQLite3Connection] query \(queryString), bindings \(whereClauses.map { "\($0.value)" })")
        #endif
        
        let executeStatus = sqlite3_step(queryStatement)
        guard executeStatus == SQLITE_DONE else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryExecutionFailed(query: queryString,
                                                 status: executeStatus,
                                                 message: message)
        }
        
        return Int(sqlite3_changes(connection))
    }
    
    public func set<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        to value: Value
    ) throws -> Self {
        guard self.queryStatement == nil else {
            throw CryoError.modifyingFinalizedQuery
        }
        
        self.setClauses.append(.init(columnName: columnName, value: try .init(value: value)))
        return self
    }
    
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) throws -> Self {
        guard self.queryStatement == nil else {
            throw CryoError.modifyingFinalizedQuery
        }
        
        self.whereClauses.append(.init(columnName: columnName,
                                       operation: operation,
                                       value: try .init(value: value)))
        return self
    }
}
