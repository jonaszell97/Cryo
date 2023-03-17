
import Foundation
import SQLite3

public final class SQLiteCreateTableQuery<Model: CryoModel> {
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
    internal init(for: Model.Type, connection: OpaquePointer, config: CryoConfig?) throws {
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
            
            let schema = await CryoSchemaManager.shared.schema(for: Model.self)
            var columns = ""
            
            for columnDetails in schema.columns {
                columns += ",\n    \(columnDetails.columnName) \(SQLiteAdaptor.sqliteTypeName(for: columnDetails.type))"
            }
            
            let result = """
CREATE TABLE IF NOT EXISTS \(Model.tableName)(
    _cryo_key TEXT NOT NULL UNIQUE,
    _cryo_created TEXT NOT NULL,
    _cryo_modified TEXT NOT NULL\(columns)
);
"""
            
            self.completeQueryString = result
            return result
        }
    }
}

extension SQLiteCreateTableQuery {
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
        
        self.queryStatement = queryStatement
        return queryStatement
    }
}

extension SQLiteCreateTableQuery: CryoQuery {
    public typealias Result = Void
    
    public func execute() async throws {
        let queryStatement = try await self.compiledQuery()
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        #if DEBUG
        config?.log?(.debug, "[SQLite3Connection] query \(await queryString)")
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
        
        // Initialize the CryoSchema
        _ = await CryoSchemaManager.shared.schema(for: Model.self)
    }
}
