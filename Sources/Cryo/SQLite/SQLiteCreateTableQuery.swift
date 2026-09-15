
import Foundation
import SQLite3

public final class SQLiteCreateTableQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedSQLiteCreateTableQuery
    
    /// Create a CREATE TABLE query.
    internal init(for: Model.Type, connection: OpaquePointer, config: CryoConfig?) throws {
        self.untypedQuery = try .init(for: Model.self, connection: connection, config: config)
    }
}

extension SQLiteCreateTableQuery: CryoCreateTableQuery {
    public var queryString: String {
        untypedQuery.queryString
    }
    
    public func execute() throws {
        try untypedQuery.execute()
    }
}

internal class UntypedSQLiteCreateTableQuery {
    private let schema: CryoSchema
    /// The model type.
    let modelType: any CryoModel.Type
    
    /// The complete query string.
    var completeQueryString: String? = nil
    
    /// The compiled query statement.
    var queryStatement: OpaquePointer? = nil
    
    /// The SQLite connection.
    let connection: OpaquePointer
    
    #if DEBUG
    let config: CryoConfig?
    #endif
    
    /// Create a CREATE TABLE query.
    internal init(for modelType: any CryoModel.Type, connection: OpaquePointer, config: CryoConfig?) throws {
        self.connection = connection
        self.schema = try CryoSchemaManager.shared.schema(for: modelType)
        self.modelType = modelType
        
        #if DEBUG
        self.config = config
        #endif
    }
    
    /// The complete query string.
    public var queryString: String {
        if let completeQueryString {
            return completeQueryString
        }
        
        var columns = ""
        
        for columnDetails in schema.columns {
            switch columnDetails {
            case .value(let columnName, _, _, _):
                let specifiers: String
                if columnName == "id" {
                    specifiers = " NOT NULL PRIMARY KEY"
                }
                else {
                    specifiers = ""
                }
                
                columns += ",\n    \(SQLiteAdaptor.quoteIdentifier(columnName)) \(SQLiteAdaptor.sqliteTypeName(for: columnDetails))\(specifiers)"
            case .oneToOneRelation(let columnName, let modelType, _):
                columns += ",\n    \(SQLiteAdaptor.quoteIdentifier(columnName)) TEXT NOT NULL"
                columns += ",\n    FOREIGN KEY(\(SQLiteAdaptor.quoteIdentifier(columnName))) REFERENCES \(SQLiteAdaptor.quoteIdentifier(modelType.tableName))(\"id\")"
            }
        }
        
        let result = """
CREATE TABLE IF NOT EXISTS \(SQLiteAdaptor.quoteIdentifier(modelType.tableName))(
    "_cryo_created" TEXT NOT NULL,
    "_cryo_modified" TEXT NOT NULL\(columns)
);
"""
        
        self.completeQueryString = result
        return result
    }
}

extension UntypedSQLiteCreateTableQuery {
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
        
        self.queryStatement = queryStatement
        return queryStatement
    }
}

extension UntypedSQLiteCreateTableQuery {
    public typealias Result = Void
    
    public func execute() throws {
        let queryStatement = try self.compiledQuery()
        defer {
            sqlite3_finalize(queryStatement)
            self.queryStatement = nil
        }
        
        #if DEBUG
        config?.log?(.debug, "[SQLite3Connection] \(queryString)")
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

        try addMissingColumns()
    }

    private func addMissingColumns() throws {
        let pragma = "PRAGMA table_info(\(SQLiteAdaptor.quoteIdentifier(modelType.tableName)))"
        var statement: OpaquePointer?
        let prepareStatus = sqlite3_prepare_v2(connection, pragma, -1, &statement, nil)
        guard prepareStatus == SQLITE_OK, let statement else {
            throw CryoError.queryCompilationFailed(query: pragma, status: prepareStatus,
                                                   message: sqlite3_errmsg(connection).map(String.init(cString:)))
        }
        defer { sqlite3_finalize(statement) }

        var existing = Set<String>()
        var status = sqlite3_step(statement)
        while status == SQLITE_ROW {
            if let name = sqlite3_column_text(statement, 1) {
                existing.insert(String(cString: name))
            }
            status = sqlite3_step(statement)
        }
        guard status == SQLITE_DONE else {
            throw CryoError.queryExecutionFailed(query: pragma, status: status,
                                                 message: sqlite3_errmsg(connection).map(String.init(cString:)))
        }

        for column in schema.columns where !existing.contains(column.columnName) {
            // Existing rows cannot satisfy a newly-added NOT NULL relation, so migrations add
            // columns as nullable. Model decoding will still reject missing required values.
            let sql = "ALTER TABLE \(SQLiteAdaptor.quoteIdentifier(modelType.tableName)) ADD COLUMN \(SQLiteAdaptor.quoteIdentifier(column.columnName)) \(SQLiteAdaptor.sqliteTypeName(for: column))"
            var errorMessage: UnsafeMutablePointer<CChar>?
            let alterStatus = sqlite3_exec(connection, sql, nil, nil, &errorMessage)
            let message = errorMessage.map { String(cString: $0) }
            sqlite3_free(errorMessage)
            guard alterStatus == SQLITE_OK else {
                throw CryoError.queryExecutionFailed(query: sql, status: alterStatus, message: message)
            }
        }
    }
}
