
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
    
    public func execute() async throws {
        try await untypedQuery.execute()
    }
}

internal class UntypedSQLiteCreateTableQuery {
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
        
        let schema = CryoSchemaManager.shared.schema(for: modelType)
        var columns = ""
        
        for columnDetails in schema.columns {
            switch columnDetails {
            case .value(let columnName, _, _):
                let specifiers: String
                if columnName == "id" {
                    specifiers = " NOT NULL UNIQUE"
                }
                else {
                    specifiers = ""
                }
                
                columns += ",\n    \(columnName) \(SQLiteAdaptor.sqliteTypeName(for: columnDetails))\(specifiers)"
            case .oneToOneRelation(let columnName, let modelType, _):
                columns += ",\n    \(columnName) TEXT NOT NULL"
                columns += ",\n    FOREIGN KEY(\(columnName)) REFERENCES \(modelType.tableName)(id)"
            }
        }
        
        let result = """
CREATE TABLE IF NOT EXISTS \(modelType.tableName)(
    _cryo_created TEXT NOT NULL,
    _cryo_modified TEXT NOT NULL\(columns)
);
"""
        
        self.completeQueryString = result
        return result
    }
}

extension UntypedSQLiteCreateTableQuery {
    /// Get the compiled query statement.
    func compiledQuery() async throws -> OpaquePointer {
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
    
    public func execute() async throws {
        // Initialize the CryoSchema
        await CryoSchemaManager.shared.createSchema(for: modelType)
        
        let queryStatement = try await self.compiledQuery()
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        #if DEBUG
        config?.log?(.debug, "[SQLite3Connection] query \(queryString)")
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
    }
}
