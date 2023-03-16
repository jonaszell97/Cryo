
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
                columnsString = schema.map { $0.columnName }.joined(separator: ",")
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
            self.bind(queryStatement, value: whereClauses[i].value, index: Int32(i + 1))
        }
        
        self.queryStatement = queryStatement
        return queryStatement
    }
    
    /// Bind a variable.
    func bind(_ queryStatement: OpaquePointer, value: CryoQueryValue, index: Int32) {
        let stringValue: String
        switch value {
        case .integer(let value):
            sqlite3_bind_int(queryStatement, index, Int32(value))
            return
        case .double(let value):
            sqlite3_bind_double(queryStatement, index, value)
            return
        case .data(let value):
            _ = value.withUnsafeBytes { (bytes: UnsafeRawBufferPointer) in
                sqlite3_bind_blob(queryStatement, index, bytes.baseAddress, Int32(bytes.count), nil)
            }
            return
        case .string(let value):
            stringValue = value
        case .date(let value):
            stringValue = ISO8601DateFormatter().string(from: value)
        case .asset(let value):
            stringValue = value.absoluteString
        }
        
        _ = stringValue.utf8CString.withUnsafeBufferPointer { buffer in
            sqlite3_bind_text(queryStatement, index, buffer.baseAddress, -1, SQLiteAdaptor.SQLITE_TRANSIENT)
        }
    }
    
    /// Get a result value from the given query.
    private func columnValue(_ statement: OpaquePointer, columnName: String,
                             type: CryoColumnType, index: Int32) throws -> _AnyCryoColumnValue {
        switch type {
        case .integer:
            return sqlite3_column_int(statement, index)
        case .double:
            return sqlite3_column_double(statement, index)
        case .text:
            guard let absoluteString = sqlite3_column_text(statement, index) else {
                var message: String? = nil
                if let errorPointer = sqlite3_errmsg(connection) {
                    message = String(cString: errorPointer)
                }
                
                throw CryoError.queryDecodeFailed(column: columnName, message: message)
            }
            
            return String(cString: absoluteString)
        case .date:
            guard
                let dateString = sqlite3_column_text(statement, index),
                let date = ISO8601DateFormatter().date(from: String(cString: dateString))
            else {
                var message: String? = nil
                if let errorPointer = sqlite3_errmsg(connection) {
                    message = String(cString: errorPointer)
                }
                
                throw CryoError.queryDecodeFailed(column: columnName, message: message)
            }
            
            return date
        case .data:
            let byteCount = sqlite3_column_bytes(statement, index)
            guard let blob = sqlite3_column_blob(statement, index) else {
                var message: String? = nil
                if let errorPointer = sqlite3_errmsg(connection) {
                    message = String(cString: errorPointer)
                }
                
                throw CryoError.queryDecodeFailed(column: columnName, message: message)
            }
            
            return Data(bytes: blob, count: Int(byteCount))
        case .bool:
            return sqlite3_column_int(statement, index) != 0
        case .asset:
            fatalError("not supported in SQLiteAdaptor")
        }
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
            
            for i in 0..<schema.count {
                let value = try self.columnValue(queryStatement,
                                                 columnName: schema[i].columnName,
                                                 type: schema[i].type,
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
            for i in 0..<schema.count {
                data[schema[i].columnName] = row[i]
            }
            
            values.append(try .init(from: CryoModelDecoder(data: data)))
        }
        
        return values
    }
    
    /// Attach a WHERE clause to this query.
    @discardableResult public func `where`<Value: _AnyCryoColumnValue>(
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
