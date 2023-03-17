
import Foundation
import SQLite3

public final class SQLiteInsertQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedSQLiteInsertQuery
    
    /// Create an INSERT query.
    internal init(id: String, value: Model, replace: Bool, connection: OpaquePointer, config: CryoConfig?) throws {
        self.untypedQuery = try .init(id: id, value: value, replace: replace, connection: connection, config: config)
    }
    
    /// The database operation for this query.
    var operation: DatabaseOperation {
        get async throws {
            var data = [DatabaseOperationValue]()
            let schema = await CryoSchemaManager.shared.schema(for: Model.self)
            
            for column in schema.columns {
                data.append(.init(columnName: column.columnName, value: try .init(value: column.getValue(untypedQuery.value))))
            }
            
            return .insert(date: .now, tableName: Model.tableName, rowId: untypedQuery.id, data: data)
        }
    }
}

extension SQLiteInsertQuery: CryoInsertQuery {
    public var queryString: String {
        get async {
            await untypedQuery.queryString
        }
    }
    
    @discardableResult public func execute() async throws -> Bool {
        try await untypedQuery.execute()
    }
}

internal class UntypedSQLiteInsertQuery {
    /// The ID to insert the value with.
    let id: String
    
    /// The model value to insert.
    let value: any CryoModel
    
    /// Whether to replace an existing value with the same key
    let replace: Bool
    
    /// The creation date of this query.
    let created: Date
    
    /// The complete query string.
    var completeQueryString: String? = nil
    
    /// The compiled query statement.
    var queryStatement: OpaquePointer? = nil
    
    /// The SQLite connection.
    let connection: OpaquePointer
    
    #if DEBUG
    let config: CryoConfig?
    #endif
    
    /// Create an INSERT query.
    internal init(id: String, value: any CryoModel, replace: Bool, connection: OpaquePointer, config: CryoConfig?) throws {
        self.id = id
        self.value = value
        self.replace = replace
        self.created = .now
        self.connection = connection
        
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
            
            let modelType = type(of: value)
            let schema = await CryoSchemaManager.shared.schema(for: modelType)
            let columns: [String] = schema.columns.map { $0.columnName }
            
            let result = """
INSERT \(replace ? "OR REPLACE " : "")INTO \(modelType.tableName)(_cryo_key,_cryo_created,_cryo_modified,\(columns.joined(separator: ",")))
    VALUES (?,?,?,\(columns.map { _ in "?" }.joined(separator: ",")));
"""
            
            self.completeQueryString = result
            return result
        }
    }
}

extension UntypedSQLiteInsertQuery {
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
        
        let schema = await CryoSchemaManager.shared.schema(for: type(of: value))
        
        var bindings: [CryoQueryValue] = [.string(value: id), .date(value: created), .date(value: created)]
        bindings.append(contentsOf: try schema.columns.map { try .init(value: $0.getValue(value)) })
        
        for i in 0..<bindings.count {
            SQLiteAdaptor.bind(queryStatement, value: bindings[i], index: Int32(i + 1))
        }
        
        self.queryStatement = queryStatement
        return queryStatement
    }
}

extension UntypedSQLiteInsertQuery {
    @discardableResult public func execute() async throws -> Bool {
        let queryStatement = try await self.compiledQuery()
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        #if DEBUG
        config?.log?(.debug, "[SQLite3Connection] query \(await queryString)")
        #endif
        
        let executeStatus = sqlite3_step(queryStatement)
        guard executeStatus == SQLITE_DONE else {
            // Check if UNIQUE constraint failed
            if executeStatus == SQLITE_CONSTRAINT {
                if let errorPointer = sqlite3_errmsg(connection) {
                    let message = String(cString: errorPointer)
                    if message.contains("UNIQUE") && message.contains("_cryo_key") {
                        throw CryoError.duplicateId(id: self.id)
                    }
                }
            }
            
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryExecutionFailed(query: await queryString,
                                                 status: executeStatus,
                                                 message: message)
        }
        
        return sqlite3_changes(connection) > 0
    }
}
