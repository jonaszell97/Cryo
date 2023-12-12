
import Foundation
import SQLite3

public final class SQLiteInsertQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedSQLiteInsertQuery
    
    /// Create an INSERT query.
    internal init(id: String, value: Model, replace: Bool, connection: OpaquePointer, config: CryoConfig?) throws {
        self.untypedQuery = try .init(id: id, value: value, replace: replace, connection: connection, config: config)
    }
}

extension SQLiteInsertQuery: CryoInsertQuery {
    public var id: String { untypedQuery.id }
    public var value: Model { untypedQuery.value as! Model }
    
    public var queryString: String {
        untypedQuery.queryString
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
        if let completeQueryString {
            return completeQueryString
        }
        
        let modelType = type(of: value)
        let schema = CryoSchemaManager.shared.schema(for: modelType)
        let columns: [String] = schema.columns.map { $0.columnName }
        
        let result = """
INSERT \(replace ? "OR REPLACE " : "")INTO \(modelType.tableName)(_cryo_created,_cryo_modified,\(columns.joined(separator: ",")))
    VALUES (?,?,\(columns.map { _ in "?" }.joined(separator: ",")));
"""
        
        self.completeQueryString = result
        return result
    }
    
    fileprivate var logQueryString: String {
        let modelType = type(of: value)
        let schema = CryoSchemaManager.shared.schema(for: modelType)
        let columns: [String] = schema.columns.map { $0.columnName }
        
        let result = """
INSERT \(replace ? "OR REPLACE " : "")INTO \(modelType.tableName)(_cryo_created,_cryo_modified,\(columns.joined(separator: ",")))
    VALUES (?,?,\(columns.map { "\($0)" }.joined(separator: ",")));
"""
        
        self.completeQueryString = result
        return result
    }
}

extension UntypedSQLiteInsertQuery {
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
        
        let schema = CryoSchemaManager.shared.schema(for: type(of: value))
        
        var bindings: [CryoQueryValue] = [.date(value: created), .date(value: created)]
        bindings.append(contentsOf: try schema.columns.map { try .init(value: $0.getValue(value)) })
        
        for i in 0..<bindings.count {
            SQLiteAdaptor.bind(queryStatement, value: bindings[i], index: Int32(i + 1))
        }
        
        #if DEBUG
        config?.log?(.debug, "[SQLite3Connection] \(queryString), bindings \(bindings.map { "\($0)" }.joined(separator: ", "))")
        #endif
        
        self.queryStatement = queryStatement
        return queryStatement
    }
}

extension UntypedSQLiteInsertQuery {
    @discardableResult public func execute() async throws -> Bool {
        let queryStatement = try self.compiledQuery()
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        let executeStatus = sqlite3_step(queryStatement)
        guard executeStatus == SQLITE_DONE else {
            // Check if UNIQUE constraint failed
            if executeStatus == SQLITE_CONSTRAINT {
                if let errorPointer = sqlite3_errmsg(connection) {
                    let message = String(cString: errorPointer)
                    if message.contains("UNIQUE") && message.contains("id") {
                        throw CryoError.duplicateId(id: self.id)
                    }
                    if message.contains("FOREIGN") {
                        throw CryoError.foreignKeyConstraintFailed(tableName: type(of: value).tableName,
                                                                   message: message)
                    }
                }
            }
            
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryExecutionFailed(query: queryString,
                                                 status: executeStatus,
                                                 message: message)
        }
        
        return sqlite3_changes(connection) > 0
    }
}
