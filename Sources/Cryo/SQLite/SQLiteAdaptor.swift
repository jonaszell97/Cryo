
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
    
    /// The registered update hooks.
    var updateHooks: [String: [() async throws -> Void]] = [:]
    
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
    
    /// Enable foreign keys.
    public func enableForeignKeys() async throws {
        let queryString = "PRAGMA foreign_keys = ON"
        var queryStatement: OpaquePointer?
        
        let prepareStatus = sqlite3_prepare_v3(db.connection, queryString, -1, 0, &queryStatement, nil)
        guard prepareStatus == SQLITE_OK, let queryStatement else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(db.connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryCompilationFailed(query: queryString, status: prepareStatus, message: message)
        }
        
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        #if DEBUG
        config?.log?(.debug, "[SQLite3Connection] enabling foreign keys")
        #endif
        
        let executeStatus = sqlite3_step(queryStatement)
        guard executeStatus != SQLITE_DONE else {
            return
        }
        
        var message: String? = nil
        if let errorPointer = sqlite3_errmsg(db.connection) {
            message = String(cString: errorPointer)
        }
        
        throw CryoError.queryExecutionFailed(query: queryString,
                                             status: executeStatus,
                                             message: message)
    }
}

// MARK: Queries

extension SQLiteAdaptor: CryoDatabaseAdaptor {
    /// Create a table if it does not exist yet.
    public func createTable<Model: CryoModel>(for model: Model.Type) async throws -> any CryoCreateTableQuery<Model> {
        try SQLiteCreateTableQuery(for: model, connection: db.connection, config: config)
    }
    
    func createTable(modelType: any CryoModel.Type) async throws -> UntypedSQLiteCreateTableQuery {
        try UntypedSQLiteCreateTableQuery(for: modelType, connection: db.connection, config: config)
    }
    
    public func select<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> any CryoSelectQuery<Model> {
        var query: SQLiteSelectQuery<Model> = try SQLiteSelectQuery(connection: db.connection, config: config)
        if let id {
            query = try query.where("id", operation: .equals, value: id)
        }
        
        return query
    }
    
    public func insert<Model: CryoModel>(_ value: Model, replace: Bool = true) async throws -> SQLiteInsertQuery<Model> {
        try SQLiteInsertQuery(id: value.id, value: value, replace: replace, connection: db.connection, config: config)
    }
    
    public func update<Model: CryoModel>(id: String? = nil, from modelType: Model.Type) async throws -> SQLiteUpdateQuery<Model> {
        try SQLiteUpdateQuery(from: modelType, id: id, connection: db.connection, config: config)
    }
    
    public func delete<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> SQLiteDeleteQuery<Model> {
        try SQLiteDeleteQuery(id: id, connection: db.connection, config: config)
    }
}

extension SQLiteAdaptor: ResilientStoreBackend {
    func execute(operation: DatabaseOperation) async throws {
        switch operation {
        case .insert(_, let tableName, let rowId, let data):
            guard let schema = CryoSchemaManager.shared.schema(tableName: tableName) else {
                throw CryoError.schemaNotInitialized(tableName: tableName)
            }
            
            var modelData = [String: _AnyCryoColumnValue]()
            for item in data {
                modelData[item.columnName] = item.value.columnValue
            }
            
            let model = try schema.create(modelData)
            _ = try await UntypedSQLiteInsertQuery(id: rowId, value: model, replace: false, connection: db.connection, config: config)
                .execute()
        case .update(_, let tableName, let rowId, let setClauses, let whereClauses):
            guard let schema = CryoSchemaManager.shared.schema(tableName: tableName) else {
                throw CryoError.schemaNotInitialized(tableName: tableName)
            }
            
            let query = try UntypedSQLiteUpdateQuery(id: rowId, modelType: schema.`self`, connection: db.connection, config: config)
            for setClause in setClauses {
                _ = try query.set(setClause.columnName, to: setClause.value.columnValue)
            }
            for whereClause in whereClauses {
                _ = try query.where(whereClause.columnName, operation: whereClause.operation, value: whereClause.value.columnValue)
            }
            
            _ = try await query.execute()
            break
        case .delete(_, let tableName, let rowId, let whereClauses):
            guard let schema = CryoSchemaManager.shared.schema(tableName: tableName) else {
                throw CryoError.schemaNotInitialized(tableName: tableName)
            }
            
            let query = try UntypedSQLiteDeleteQuery(id: rowId, modelType: schema.`self`, connection: db.connection, config: config)
            for whereClause in whereClauses {
                _ = try query.where(whereClause.columnName, operation: whereClause.operation, value: whereClause.value.columnValue)
            }
            
            _ = try await query.execute()
        }
    }
    
    public nonisolated var isAvailable: Bool { true }
    
    public func ensureAvailability() async throws {
        
    }
    
    public nonisolated func observeAvailabilityChanges(_ callback: @escaping (Bool) -> Void) {
        
    }
}

// MARK: Update hook

extension SQLiteAdaptor {
    /// Register a change callback.
    public func registerChangeListener<Model: CryoModel>(for modelType: Model.Type,
                                                         listener: @escaping () -> Void) {
        self.registerChangeListener(tableName: modelType.tableName, listener: listener)
    }
    
    /// Register a change callback.
    public func registerChangeListener(tableName: String, listener: @escaping () async throws -> Void) {
        if updateHooks.isEmpty {
            self.updateHook { op, db, table, rowid in
                Task {
                    try await self.updateHookCallback(operation: op, db: db, table: table, rowid: rowid)
                }
            }
        }
        
        if var hooks = updateHooks[tableName] {
            hooks.append(listener)
            updateHooks[tableName] = hooks
        }
        else {
            updateHooks[tableName] = [listener]
        }
    }
}

// Partially taken from SQLite.swift
fileprivate extension SQLiteAdaptor {
    /// An SQL operation passed to update callbacks.
    enum Operation {
        
        /// An INSERT operation.
        case insert
        
        /// An UPDATE operation.
        case update
        
        /// A DELETE operation.
        case delete
        
        fileprivate init(rawValue:Int32) {
            switch rawValue {
            case SQLITE_INSERT:
                self = .insert
            case SQLITE_UPDATE:
                self = .update
            case SQLITE_DELETE:
                self = .delete
            default:
                fatalError("unhandled operation code: \(rawValue)")
            }
        }
    }
    
    func updateHookCallback(operation: Operation, db: String, table: String, rowid: Int64) async throws {
        guard let hooks = updateHooks[table] else {
            return
        }
        
        for hook in hooks {
            try await hook()
        }
    }
    
    /// Registers a callback to be invoked whenever a row is inserted, updated, or deleted in a rowid table.
    func updateHook(_ callback: ((_ operation: Operation, _ db: String, _ table: String, _ rowid: Int64) -> Void)?) {
        guard let callback = callback else {
            sqlite3_update_hook(db.connection, nil, nil)
            return
        }
        
        let box: UpdateHook = {
            callback(
                Operation(rawValue: $0),
                String(cString: $1),
                String(cString: $2),
                $3
            )
        }
        
        sqlite3_update_hook(db.connection, { callback, operation, db, table, rowid in
            unsafeBitCast(callback, to: UpdateHook.self)(operation, db!, table!, rowid)
        }, unsafeBitCast(box, to: UnsafeMutableRawPointer.self))
    }
    
    typealias UpdateHook = @convention(block) (Int32, UnsafePointer<Int8>, UnsafePointer<Int8>, Int64) -> Void
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
    static func sqliteTypeName(for column: CryoSchemaColumn) -> String {
        switch column {
        case .value(_, let type, _):
            switch type {
            case .integer:
                return "INTEGER"
            case .double:
                return "NUMERIC"
            case .text:
                return "TEXT"
            case .date:
                return "TEXT"
            case .bool:
                return "INTEGER"
            case .data:
                return "BLOB"
            case .asset:
                return "BLOB"
            }
        case .oneToOneRelation:
            return "TEXT"
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
