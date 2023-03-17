
import Foundation
import SQLite3

public final class SQLiteSelectQuery<Model: CryoModel> {
    /// The columns to select.
    let columns: [String]?
    
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
    internal init(columns: [String]? = nil, connection: OpaquePointer, config: CryoConfig?) throws {
        self.connection = connection
        self.columns = columns
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
            
            let columnsString: String
            if let columns {
                columnsString = columns.joined(separator: ",")
            }
            else {
                let schema = await CryoSchemaManager.shared.schema(for: Model.self)
                columnsString = schema.columns.map { $0.columnName }.joined(separator: ",")
            }
            
            var result = "SELECT \(columnsString) FROM \(Model.tableName)"
            for i in 0..<whereClauses.count {
                if i == 0 {
                    result += " WHERE "
                }
                else {
                    result += " AND "
                }
                
                result += "\(whereClauses[i].columnName) \(SQLiteAdaptor.formatOperator(whereClauses[i].operation)) ?"
            }
            
            self.completeQueryString = result
            return result
        }
    }
}

extension SQLiteSelectQuery {
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
        
        for i in 0..<whereClauses.count {
            SQLiteAdaptor.bind(queryStatement, value: whereClauses[i].value, index: Int32(i + 1))
        }
        
        self.queryStatement = queryStatement
        return queryStatement
    }
}

extension SQLiteSelectQuery: CryoSelectQuery {
    public func execute() async throws -> [Model] {
        let queryStatement = try await self.compiledQuery()
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        #if DEBUG
        config?.log?(.debug, "[SQLite3Connection] query \(await queryString), bindings \(whereClauses.map { "\($0.value)" })")
        #endif
        
        let schema = await CryoSchemaManager.shared.schema(for: Model.self)
        
        var executeStatus = sqlite3_step(queryStatement)
        var rows = [[any _AnyCryoColumnValue]]()
        
        while executeStatus == SQLITE_ROW {
            var row = [any _AnyCryoColumnValue]()
            
            for i in 0..<schema.columns.count {
                let value = try SQLiteAdaptor.columnValue(queryStatement,
                                                          connection: connection,
                                                          columnName: schema.columns[i].columnName,
                                                          type: schema.columns[i].type,
                                                          index: Int32(i))
                
                row.append(value)
            }
            
            rows.append(row)
            executeStatus = sqlite3_step(queryStatement)
        }
        
        guard executeStatus == SQLITE_DONE else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryExecutionFailed(query: await queryString,
                                                 status: executeStatus,
                                                 message: message)
        }
        
        var values = [Model]()
        for row in rows {
            var data = [String: _AnyCryoColumnValue]()
            for i in 0..<schema.columns.count {
                data[schema.columns[i].columnName] = row[i]
            }
            
            values.append(try .init(from: CryoModelDecoder(data: data)))
        }
        
        return values
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
    
    public typealias Result = [Model]
}
