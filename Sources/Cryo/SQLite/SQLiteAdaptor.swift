
import Foundation
import SQLite3

private actor SQLiteAsyncMutex {
    private var isLocked = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func lock() async {
        if !isLocked {
            isLocked = true
            return
        }
        await withCheckedContinuation { waiters.append($0) }
    }

    func unlock() {
        if waiters.isEmpty {
            isLocked = false
        } else {
            waiters.removeFirst().resume()
        }
    }
}

fileprivate typealias SQLiteUpdateHook = @convention(block) (
    Int32, UnsafePointer<Int8>, UnsafePointer<Int8>, Int64
) -> Void

fileprivate final class SQLite3Connection {
    /// The pointer to the database connection object.
    let connection: OpaquePointer
    
    /// The cryo config.
    let config: CryoConfig?
    
    /// Create a database connection.
    init(databaseUrl: URL, config: CryoConfig?) throws {
        var connection: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        let status = sqlite3_open_v2(databaseUrl.path, &connection, flags, nil)
        
        guard status == SQLITE_OK, let connection else {
            if let connection { sqlite3_close(connection) }
            throw CryoError.databaseConnectionFailed(dbName: databaseUrl.path, status: status)
        }

        sqlite3_busy_timeout(connection, 5_000)
        sqlite3_extended_result_codes(connection, 1)
        
        self.connection = connection
        self.config = config
    }
    
    /// Close the connection.
    deinit {
        sqlite3_close(connection)
    }
}

/// Implementation of ``CryoDatabaseAdaptor`` using a local SQLite database.
public final class SQLiteAdaptor {
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

    private let updateHooksLock = NSLock()
    private var updateHookBox: SQLiteUpdateHook?
    private let transactionMutex = SQLiteAsyncMutex()
    
    /// Create an SQLite adaptor.
    public init(databaseUrl: URL, config: CryoConfig? = nil) throws {
        self.databaseUrl = databaseUrl
        self.config = config
        self.db = try .init(databaseUrl: databaseUrl, config: config)
    }

    deinit {
        sqlite3_update_hook(db.connection, nil, nil)
        updateHookBox = nil
    }
}

extension SQLiteAdaptor {
    /// Execute operations in a transaction.
    public func transaction(_ operations: () async throws -> Void) async throws {
        await transactionMutex.lock()
        do {
            try executeSQL("BEGIN IMMEDIATE")
            do {
                try await operations()
                try executeSQL("COMMIT")
            } catch {
                try? executeSQL("ROLLBACK")
                throw error
            }
            await transactionMutex.unlock()
        } catch {
            await transactionMutex.unlock()
            throw error
        }
    }
    
    /// Execute operations with another attached database.
    public func withAttachedDatabase(databaseUrl: URL, _ operations: (String) async throws -> Void) async throws {
        await transactionMutex.lock()
        let alias = "cryo_attached"
        do {
            try attachDatabase(at: databaseUrl, alias: alias)

            do {
                try await operations(alias)
                try executeSQL("DETACH DATABASE \(Self.quoteIdentifier(alias))")
            } catch {
                try? executeSQL("DETACH DATABASE \(Self.quoteIdentifier(alias))")
                throw error
            }
            await transactionMutex.unlock()
        } catch {
            await transactionMutex.unlock()
            throw error
        }
    }

    private func attachDatabase(at databaseUrl: URL, alias: String) throws {
        var statement: OpaquePointer?
        let sql = "ATTACH DATABASE ? AS \(Self.quoteIdentifier(alias))"
        let status = sqlite3_prepare_v2(db.connection, sql, -1, &statement, nil)
        guard status == SQLITE_OK, let statement else {
            throw queryError(sql: sql, status: status, compilation: true)
        }
        defer { sqlite3_finalize(statement) }
        let bindStatus = databaseUrl.path.withCString {
            sqlite3_bind_text(statement, 1, $0, -1, Self.SQLITE_TRANSIENT)
        }
        guard bindStatus == SQLITE_OK else {
            throw queryError(sql: sql, status: bindStatus, compilation: false)
        }
        let stepStatus = sqlite3_step(statement)
        guard stepStatus == SQLITE_DONE else {
            throw queryError(sql: sql, status: stepStatus, compilation: false)
        }
    }

    private func executeSQL(_ sql: String) throws {
        var errorMessage: UnsafeMutablePointer<CChar>?
        let status = sqlite3_exec(db.connection, sql, nil, nil, &errorMessage)
        defer { sqlite3_free(errorMessage) }
        guard status == SQLITE_OK else {
            throw CryoError.queryExecutionFailed(
                query: sql, status: status,
                message: errorMessage.map { String(cString: $0) }
            )
        }
    }

    private func queryError(sql: String, status: Int32, compilation: Bool) -> CryoError {
        let message = sqlite3_errmsg(db.connection).map { String(cString: $0) }
        return compilation
            ? .queryCompilationFailed(query: sql, status: status, message: message)
            : .queryExecutionFailed(query: sql, status: status, message: message)
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

// MARK: Resetting

extension SQLiteAdaptor {
    /// Clear the database.
    func clearTable(modelType: any CryoModel.Type) throws {
        try UntypedSQLiteDeleteQuery(id: nil, modelType: modelType, connection: db.connection, config: config).execute()
    }
}

// MARK: Queries

extension SQLiteAdaptor: CryoDatabaseAdaptor {
    /// Create a table if it does not exist yet.
    public func createTable<Model: CryoModel>(for model: Model.Type) async throws -> any CryoCreateTableQuery<Model> {
        // Initialize the CryoSchema
        try CryoSchemaManager.shared.createSchema(for: model)
        return try SQLiteCreateTableQuery(for: model, connection: db.connection, config: config)
    }
    
    func createTable(modelType: any CryoModel.Type) async throws -> UntypedSQLiteCreateTableQuery {
        // Initialize the CryoSchema
        try CryoSchemaManager.shared.createSchema(for: modelType)
        return try UntypedSQLiteCreateTableQuery(for: modelType, connection: db.connection, config: config)
    }
    
    public func select<Model: CryoModel>(id: String? = nil, from: Model.Type) throws -> SQLiteSelectQuery<Model> {
        var query: SQLiteSelectQuery<Model> = try SQLiteSelectQuery(connection: db.connection, config: config)
        if let id {
            query = try query.where("id", operation: .equals, value: id)
        }
        
        return query
    }
    
    public func insert<Model: CryoModel>(_ value: Model, replace: Bool = true) throws -> SQLiteInsertQuery<Model> {
        try SQLiteInsertQuery(id: value.id, value: value, replace: replace, connection: db.connection, config: config)
    }
    
    public func update<Model: CryoModel>(id: String? = nil, from modelType: Model.Type) throws -> SQLiteUpdateQuery<Model> {
        try SQLiteUpdateQuery(from: modelType, id: id, connection: db.connection, config: config)
    }
    
    public func delete<Model: CryoModel>(id: String? = nil, from: Model.Type) throws -> SQLiteDeleteQuery<Model> {
        try SQLiteDeleteQuery(id: id, connection: db.connection, config: config)
    }
}

extension SQLiteAdaptor: ResilientStoreBackend {
    func execute(operation: DatabaseOperation) async throws {
        switch operation {
        case .insert(_, let tableName, let rowId, let data, let replace):
            let schema = try CryoSchemaManager.shared.schema(tableName: tableName)
            
            var modelData = [String: CryoColumnValueWrapper]()
            for item in data {
                modelData[item.columnName] = .init(value: item.value.columnValue)
            }
            
            let model = try schema.create(modelData)
            _ = try UntypedSQLiteInsertQuery(id: rowId, value: model, replace: replace, connection: db.connection, config: config)
                .execute()
        case .update(_, let tableName, let rowId, let setClauses, let whereClauses):
            let schema = try CryoSchemaManager.shared.schema(tableName: tableName)
            
            let query = try UntypedSQLiteUpdateQuery(id: rowId, modelType: schema.`self`, connection: db.connection, config: config)
            for setClause in setClauses {
                _ = try query.set(setClause.columnName, to: setClause.value.columnValue)
            }
            for whereClause in whereClauses {
                _ = try query.where(whereClause.columnName, operation: whereClause.operation, value: whereClause.value.columnValue)
            }
            
            _ = try query.execute()
            break
        case .delete(_, let tableName, let rowId, let whereClauses):
            let schema = try CryoSchemaManager.shared.schema(tableName: tableName)
            
            let query = try UntypedSQLiteDeleteQuery(id: rowId, modelType: schema.`self`, connection: db.connection, config: config)
            for whereClause in whereClauses {
                _ = try query.where(whereClause.columnName, operation: whereClause.operation, value: whereClause.value.columnValue)
            }
            
            _ = try query.execute()
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
        updateHooksLock.lock()
        defer { updateHooksLock.unlock() }
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
        let hooks = snapshotHooks(for: table)
        guard let hooks else {
            return
        }

        for hook in hooks {
            try await hook()
        }
    }

    func snapshotHooks(for table: String) -> [() async throws -> Void]? {
        updateHooksLock.lock()
        defer { updateHooksLock.unlock() }
        let hooks = updateHooks[table]
        return hooks
    }
    
    /// Registers a callback to be invoked whenever a row is inserted, updated, or deleted in a rowid table.
    func updateHook(_ callback: ((_ operation: Operation, _ db: String, _ table: String, _ rowid: Int64) -> Void)?) {
        guard let callback = callback else {
            sqlite3_update_hook(db.connection, nil, nil)
            return
        }
        
        let box: SQLiteUpdateHook = {
            callback(
                Operation(rawValue: $0),
                String(cString: $1),
                String(cString: $2),
                $3
            )
        }
        
        updateHookBox = box
        sqlite3_update_hook(db.connection, { context, operation, db, table, rowid in
            guard let context, let db, let table else { return }
            let adaptor = Unmanaged<SQLiteAdaptor>.fromOpaque(context).takeUnretainedValue()
            adaptor.updateHookBox?(operation, db, table, rowid)
        }, Unmanaged.passUnretained(self).toOpaque())
    }
}

extension SQLiteAdaptor {
    static let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
    static let constraintForeignKey = SQLITE_CONSTRAINT | (3 << 8)
    static let constraintPrimaryKey = SQLITE_CONSTRAINT | (6 << 8)
    static let constraintUnique = SQLITE_CONSTRAINT | (8 << 8)
    static let metadataColumnCount: Int = 3

    private static let dateFormatterLock = NSLock()
    private static let fractionalDateFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()
    private static let legacyDateFormatter = ISO8601DateFormatter()

    static func quoteIdentifier(_ identifier: String) -> String {
        "\"\(identifier.replacingOccurrences(of: "\"", with: "\"\""))\""
    }

    static func string(from date: Date) -> String {
        dateFormatterLock.lock()
        defer { dateFormatterLock.unlock() }
        return fractionalDateFormatter.string(from: date)
    }

    static func date(from string: String) -> Date? {
        dateFormatterLock.lock()
        defer { dateFormatterLock.unlock() }
        return fractionalDateFormatter.date(from: string) ?? legacyDateFormatter.date(from: string)
    }
    
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
        case .value(_, let type, _, _):
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
                return "TEXT"
            }
        case .oneToOneRelation:
            return "TEXT"
        }
    }
    
    /// Bind a variable.
    static func bind(_ queryStatement: OpaquePointer, value: CryoQueryValue, index: Int32) throws {
        let stringValue: String
        let status: Int32
        switch value {
        case .null:
            status = sqlite3_bind_null(queryStatement, index)
        case .integer(let value):
            status = sqlite3_bind_int64(queryStatement, index, sqlite3_int64(value))
        case .double(let value):
            status = sqlite3_bind_double(queryStatement, index, value)
        case .data(let value):
            if value.isEmpty {
                status = sqlite3_bind_zeroblob(queryStatement, index, 0)
            } else {
                status = value.withUnsafeBytes { bytes in
                    sqlite3_bind_blob(queryStatement, index, bytes.baseAddress,
                                      Int32(bytes.count), SQLiteAdaptor.SQLITE_TRANSIENT)
                }
            }
        case .string(let value):
            stringValue = value
            status = stringValue.withCString {
                sqlite3_bind_text(queryStatement, index, $0, -1, SQLiteAdaptor.SQLITE_TRANSIENT)
            }
        case .date(let value):
            stringValue = string(from: value)
            status = stringValue.withCString {
                sqlite3_bind_text(queryStatement, index, $0, -1, SQLiteAdaptor.SQLITE_TRANSIENT)
            }
        case .asset(let value):
            stringValue = value.absoluteString
            status = stringValue.withCString {
                sqlite3_bind_text(queryStatement, index, $0, -1, SQLiteAdaptor.SQLITE_TRANSIENT)
            }
        }

        guard status == SQLITE_OK else {
            throw CryoError.queryExecutionFailed(query: "bind parameter \(index)", status: status,
                                                 message: "sqlite3_bind failed")
        }
    }
    
    /// Get a result value from the given query.
    static func columnValue(_ statement: OpaquePointer, connection: OpaquePointer, columnName: String,
                            type: CryoColumnType, index: Int32) throws -> _AnyCryoColumnValue? {
        guard sqlite3_column_type(statement, index) != SQLITE_NULL else { return nil }
        switch type {
        case .integer:
            return Int(sqlite3_column_int64(statement, index))
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
                let date = date(from: String(cString: dateString))
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
            if byteCount == 0 { return Data() }
            guard let blob = sqlite3_column_blob(statement, index) else {
                var message: String? = nil
                if let errorPointer = sqlite3_errmsg(connection) {
                    message = String(cString: errorPointer)
                }
                
                throw CryoError.queryDecodeFailed(column: columnName, message: message)
            }
            
            return Data(bytes: blob, count: Int(byteCount))
        case .bool:
            return sqlite3_column_int64(statement, index) != 0
        case .asset:
            guard let text = sqlite3_column_text(statement, index),
                  let url = URL(string: String(cString: text)) else {
                throw CryoError.queryDecodeFailed(column: columnName, message: "Invalid asset URL")
            }
            return url
        }
    }
}
