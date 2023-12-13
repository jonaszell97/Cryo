
import Foundation
import SQLite3

public final class SQLiteSelectQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedSQLiteSelectQuery
    
    /// Create a SELECT query.
    internal init(columns: [String]? = nil, connection: OpaquePointer, config: CryoConfig?) throws {
        self.untypedQuery = try .init(columns: columns, modelType: Model.self, connection: connection, config: config)
    }
}

extension SQLiteSelectQuery: CryoSelectQuery {
    public var id: String? { untypedQuery.id }
    public var whereClauses: [CryoQueryWhereClause] { untypedQuery.whereClauses }
    
    public var queryString: String {
        untypedQuery.queryString
    }
    
    @discardableResult public func execute() async throws -> [Model] {
        try await untypedQuery.execute() as! [Model]
    }
    
    
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) throws -> Self {
        _ = try untypedQuery.where(columnName, operation: operation, value: value)
        return self
    }
    
    /// Limit the number of results this query returns.
    public func limit(_ limit: Int) -> Self {
        _ = untypedQuery.limit(limit)
        return self
    }
    
    /// Define a sorting for the results of this query.
    public func sort(by columnName: String, _ order: CryoSortingOrder) -> Self {
        _ = untypedQuery.sort(by: columnName, order)
        return self
    }
}

internal class UntypedSQLiteSelectQuery {
    /// The columns to select.
    let columns: [String]?
    
    /// The model type.
    let modelType: any CryoModel.Type
    
    /// The where clauses.
    public private(set) var whereClauses: [CryoQueryWhereClause]
    
    /// The query results limit.
    var resultsLimit: Int? = nil
    
    /// The sorting clauses.
    var sortingClauses: [(String, CryoSortingOrder)] = []
    
    /// The compiled query statement.
    var queryStatement: OpaquePointer? = nil
    
    /// The complete query string.
    var completeQueryString: String? = nil
    
    /// The SQLite connection.
    let connection: OpaquePointer
    
    /// The cryo config.
    let config: CryoConfig?
    
    /// Create a SELECT query.
    internal init(columns: [String]? = nil, modelType: any CryoModel.Type, connection: OpaquePointer, config: CryoConfig?) throws {
        self.connection = connection
        self.modelType = modelType
        self.columns = columns
        self.whereClauses = []
        self.config = config
    }
    
    /// The complete query string.
    public var queryString: String {
        if let completeQueryString {
            return completeQueryString
        }
        
        let columnsString: String
        if let columns {
            columnsString = columns.joined(separator: ",")
        }
        else {
            let schema = CryoSchemaManager.shared.schema(for: modelType)
            columnsString = schema.columns.map { $0.columnName }.joined(separator: ",")
        }
        
        var result = "SELECT \(columnsString) FROM \(modelType.tableName)"
        for i in 0..<whereClauses.count {
            if i == 0 {
                result += " WHERE "
            }
            else {
                result += " AND "
            }
            
            result += "\(whereClauses[i].columnName) \(SQLiteAdaptor.formatOperator(whereClauses[i].operation)) ?"
        }
        
        if !sortingClauses.isEmpty {
            result += " ORDER BY"
            for (i, ordering) in sortingClauses.enumerated() {
                if i != 0 { result += "," }
                result += " \(ordering.0) \(ordering.1 == .ascending ? "ASC" : "DESC")"
            }
        }
        
        if let resultsLimit {
            result += " LIMIT \(resultsLimit)"
        }
        
        self.completeQueryString = result
        return result
    }
    
    /// Limit the number of results this query returns.
    public func limit(_ limit: Int) -> Self {
        self.resultsLimit = limit
        return self
    }
    
    /// Define a sorting for the results of this query.
    public func sort(by columnName: String, _ order: CryoSortingOrder) -> Self {
        self.sortingClauses.append((columnName, order))
        return self
    }
}

extension UntypedSQLiteSelectQuery {
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
        
        for i in 0..<whereClauses.count {
            SQLiteAdaptor.bind(queryStatement, value: whereClauses[i].value, index: Int32(i + 1))
        }
        
        self.queryStatement = queryStatement
        return queryStatement
    }
    
    /// Get a result value from the given query.
    func columnValue(_ queryStatement: OpaquePointer, connection: OpaquePointer,
                     column: CryoSchemaColumn, index: Int32) async throws -> _AnyCryoColumnValue? {
        switch column {
        case .value(let columnName, let type, _):
            return try SQLiteAdaptor.columnValue(queryStatement,
                                                 connection: connection,
                                                 columnName: columnName,
                                                 type: type,
                                                 index: index)
        case .oneToOneRelation(let columnName, let modelType, _):
            fatalError("relationships not implemented")
//            let id = try SQLiteAdaptor.columnValue(queryStatement,
//                                                   connection: connection,
//                                                   columnName: columnName,
//                                                   type: .text,
//                                                   index: index) as! String
//            return try await UntypedSQLiteSelectQuery(modelType: modelType, connection: connection, config: config)
//                .where("id", operation: .equals, value: id)
//                .execute().first
        }
    }
}

extension UntypedSQLiteSelectQuery {
    public var id: String? {
        guard let value = (whereClauses.first { $0.columnName == "id" }?.value) else {
            return nil
        }
        guard case .string(let id) = value else {
            return nil
        }
        
        return id
    }
    
    public func execute() async throws -> [any CryoModel] {
        let queryStatement = try self.compiledQuery()
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        #if DEBUG
        config?.log?(.debug, "[SQLite3Connection] \(queryString), bindings \(whereClauses.map { "\($0.value)" })")
        #endif
        
        let schema = CryoSchemaManager.shared.schema(for: modelType)
        
        var executeStatus = sqlite3_step(queryStatement)
        var rows = [[any _AnyCryoColumnValue]]()
        
        while executeStatus == SQLITE_ROW {
            var row = [any _AnyCryoColumnValue]()
            
            for i in 0..<schema.columns.count {
                let value = try await self.columnValue(queryStatement,
                                                       connection: connection,
                                                       column: schema.columns[i],
                                                       index: Int32(i))
                
                row.append(value!)
            }
            
            rows.append(row)
            executeStatus = sqlite3_step(queryStatement)
        }
        
        guard executeStatus == SQLITE_DONE else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryExecutionFailed(query: queryString,
                                                 status: executeStatus,
                                                 message: message)
        }
        
        var values = [any CryoModel]()
        for row in rows {
            var data = [String: _AnyCryoColumnValue]()
            for i in 0..<schema.columns.count {
                data[schema.columns[i].columnName] = row[i]
            }
            
            values.append(try schema.create(data))
        }
        
        return values
    }
    
    /// Attach a WHERE clause to this query.
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
