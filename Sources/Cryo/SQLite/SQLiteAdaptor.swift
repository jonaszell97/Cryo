
import Foundation
import SQLite3

fileprivate final class SQLite3Connection {
    /// The pointer to the database connection object.
    let connection: OpaquePointer
    
    /// The cryo config.
    let config: CryoConfig?
    
    /// Create a database connection.
    init(databaseUrl: URL, config: CryoConfig?) throws {
        var connection: OpaquePointer?
        let status = sqlite3_open(databaseUrl.absoluteString, &connection)
        
        guard status == SQLITE_OK, let connection else {
            throw CryoError.databaseConnectionFailed(dbName: databaseUrl.absoluteString, status: status)
        }
        
        self.connection = connection
        self.config = config
    }
    
    /// Close the connection.
    deinit {
        sqlite3_close(connection)
    }
}

/// Implementation of ``CryoDatabaseAdaptor`` using a local SQLite database.
public final actor SQLiteAdaptor {
    /// The database connection object.
    fileprivate let db: SQLite3Connection
    
    /// The database URL.
    let databaseUrl: URL
    
    /// The cryo config.
    let config: CryoConfig?
    
    /// The created tables.
    var createdTables = Set<String>()
    
    /// Create an SQLite adaptor.
    public init(databaseUrl: URL, config: CryoConfig? = nil) throws {
        self.databaseUrl = databaseUrl
        self.config = config
        self.db = try .init(databaseUrl: databaseUrl, config: config)
    }
}

extension SQLiteAdaptor {
    /// Execute operations in a transaction.
    public func transaction(_ operations: () async throws -> Void) async throws {
        // TODO
    }
    
    /// Execute operations with another attached database.
    public func withAttachedDatabase(databaseUrl: URL, _ operations: (String) async throws -> Void) async throws {
        // TODO
    }
}

// MARK: Queries

extension SQLiteAdaptor: CryoDatabaseAdaptor {
    /// Create a table if it does not exist yet.
    public func createTable<Model: CryoModel>(for model: Model.Type) async throws -> SQLiteCreateTableQuery<Model> {
        try SQLiteCreateTableQuery(for: model, connection: db.connection, config: config)
    }
    
    public func select<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> SQLiteSelectQuery<Model> {
        var query: SQLiteSelectQuery<Model> = try SQLiteSelectQuery(connection: db.connection, config: config)
        if let id {
            query = try await query.where("_cryo_key", operation: .equals, value: id)
        }
        
        return query
    }
    
    public func insert<Model: CryoModel>(id: String, _ value: Model, replace: Bool = true) async throws -> SQLiteInsertQuery<Model> {
        try SQLiteInsertQuery(id: id, value: value, replace: replace, connection: db.connection, config: config)
    }
    
    public func update<Model: CryoModel>(id: String? = nil) async throws -> SQLiteUpdateQuery<Model> {
        try SQLiteUpdateQuery(id: id, connection: db.connection, config: config)
    }
    
    public func delete<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> SQLiteDeleteQuery<Model> {
        try SQLiteDeleteQuery(id: id, connection: db.connection, config: config)
    }
}

extension SQLiteAdaptor {
    func execute(operation: DatabaseOperation) async throws {
        switch operation.type {
        case .insert:
            break
//            try await self.insert(id: operation.rowId, operation., replace: <#T##Bool#>)
        case .update:
            break
        case .delete:
            break
        }
    }
    
    public nonisolated var isAvailable: Bool { true }
    
    public func ensureAvailability() async throws {
        
    }
    
    public nonisolated func observeAvailabilityChanges(_ callback: @escaping (Bool) -> Void) {
        
    }
}

extension SQLiteAdaptor {
    static let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
    static let metadataColumnCount: Int = 3
    
    static func formatOperator(_ queryOperator: CryoComparisonOperator) -> String {
        switch queryOperator {
        case .equals:
            return "=="
        case .doesNotEqual:
            return "!="
        case .isGreatherThan:
            return ">"
        case .isGreatherThanOrEquals:
            return ">="
        case .isLessThan:
            return "<"
        case .isLessThanOrEquals:
            return "<="
        }
    }

    /// The SQLite type name for a Swift type.
    static func sqliteTypeName(for type: CryoColumnType) -> String {
        switch type {
        case .integer:
            return "INTEGER"
        case .double:
            return "DOUBLE"
        case .text:
            return "REAL"
        case .date:
            return "STRING"
        case .bool:
            return "INTEGER"
        case .data:
            return "BLOB"
        case .asset:
            return "BLOB"
        }
    }
    
    /// Bind a variable.
    static func bind(_ queryStatement: OpaquePointer, value: CryoQueryValue, index: Int32) {
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
    static func columnValue(_ statement: OpaquePointer, connection: OpaquePointer, columnName: String,
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
