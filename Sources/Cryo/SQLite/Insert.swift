
import Foundation
import SQLite3

public final class SQLiteInsertQuery<Model: CryoModel> {
    /// The ID to insert the value with.
    let id: String
    
    /// The model value to insert.
    let value: Model
    
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
    internal init(id: String, value: Model, connection: OpaquePointer, config: CryoConfig?) throws {
        self.id = id
        self.value = value
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
            
            let schema = await CryoSchemaManager.shared.schema(for: Model.self)
            let columns: [String] = schema.map { $0.columnName }
            
            let result = """
INSERT OR REPLACE INTO \(Model.tableName)(_cryo_key,_cryo_created,_cryo_modified,\(columns.joined(separator: ",")))
    VALUES (?,?,?,\(columns.map { _ in "?" }.joined(separator: ",")));
"""
            
            self.completeQueryString = result
            return result
        }
    }
}

extension SQLiteInsertQuery {
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
        
        let schema = await CryoSchemaManager.shared.schema(for: Model.self)
        
        var bindings: [CryoQueryValue] = [.string(value: id), .date(value: created), .date(value: created)]
        bindings.append(contentsOf: try schema.map { try .init(value: $0.getValue(value)) })
        
        for i in 0..<bindings.count {
            SQLiteAdaptor.bind(queryStatement, value: bindings[i], index: Int32(i + 1))
        }
        
        self.queryStatement = queryStatement
        return queryStatement
    }
}

extension SQLiteInsertQuery: CryoInsertQuery {
    public func execute() async throws -> Bool {
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
        
        return true
    }
}
