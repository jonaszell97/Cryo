
import Foundation
import SQLite3

fileprivate let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
fileprivate let metadataColumnCount: Int = 3

public final class SQLiteQuery {
    /// The original query string.
    public let queryString: String
    
    /// The compiled query statement.
    let queryStatement: OpaquePointer

    /// The SQLite connection.
    let connection: OpaquePointer

    /// The number of bound variables.
    var boundVariables: [any _AnyCryoColumnValue]
    
    #if DEBUG
    let config: CryoConfig?
    #endif
    
    /// Create a query.
    fileprivate init(queryString: String, connection: OpaquePointer, config: CryoConfig?) throws {
        self.queryString = queryString
        self.connection = connection
        self.boundVariables = []
        
        #if DEBUG
        self.config = config
        #endif
        
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
    }
    
    /// Finalize and execute the query.
    public func execute() throws {
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        #if DEBUG
        config?.log?(.debug, "[SQLite3Connection] query \(queryString), bindings \(boundVariables.map { "\($0)" })")
        #endif
        
        let executeStatus = sqlite3_step(queryStatement)
        guard executeStatus == SQLITE_DONE else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryExecutionFailed(query: queryString, status: executeStatus, message: message)
        }
    }
}

extension SQLiteQuery {
    /// Bind an integer value.
    @discardableResult public func bind(_ value: Int) -> Self {
        sqlite3_bind_int(queryStatement, Int32(boundVariables.count + 1), Int32(value))
        self.boundVariables.append(value)
        return self
    }
    
    /// Bind a double value.
    @discardableResult public func bind(_ value: Double) -> Self {
        sqlite3_bind_double(queryStatement, Int32(boundVariables.count + 1), value)
        self.boundVariables.append(value)
        return self
    }
    
    /// Bind a string value.
    @discardableResult public func bind(_ value: String) -> Self {
        _ = value.utf8CString.withUnsafeBufferPointer { buffer in
            sqlite3_bind_text(queryStatement, Int32(boundVariables.count + 1), buffer.baseAddress, -1, SQLITE_TRANSIENT)
        }
        
        self.boundVariables.append(value)
        return self
    }
    
    /// Bind a date value.
    @discardableResult public func bind(_ value: Date) -> Self {
        let dateString = ISO8601DateFormatter().string(from: value.dateValue)
        _ = dateString.utf8CString.withUnsafeBufferPointer { buffer in
            sqlite3_bind_text(queryStatement, Int32(boundVariables.count + 1), buffer.baseAddress, -1, SQLITE_TRANSIENT)
        }
        
        self.boundVariables.append(value)
        return self
    }
    
    /// Bind a URL value.
    @discardableResult public func bind(_ value: URL) -> Self {
        let string = value.absoluteString
        _ = string.utf8CString.withUnsafeBufferPointer { buffer in
            sqlite3_bind_text(queryStatement, Int32(boundVariables.count + 1), buffer.baseAddress, -1, SQLITE_TRANSIENT)
        }
        
        self.boundVariables.append(value)
        return self
    }
    
    /// Bind a data value.
    @discardableResult public func bind(_ value: Data) -> Self {
        _ = value.withUnsafeBytes { (bytes: UnsafeRawBufferPointer) in
            sqlite3_bind_blob(queryStatement, Int32(boundVariables.count + 1), bytes.baseAddress, Int32(bytes.count), nil)
        }
        
        self.boundVariables.append(value)
        return self
    }
    
    /// Bind a codable value.
    @discardableResult public func bind<T: Codable>(_ value: T) throws -> Self {
        let data = try JSONEncoder().encode(value)
        _ = data.withUnsafeBytes { (bytes: UnsafeRawBufferPointer) in
            sqlite3_bind_blob(queryStatement, Int32(boundVariables.count + 1), bytes.baseAddress, Int32(bytes.count), nil)
        }
        
        self.boundVariables.append(data)
        return self
    }
    
    /// Bind an array of values.
    @discardableResult public func bind(_ values: [DatabaseOperationValue]) -> Self {
        for value in values {
            switch value.value {
            case .string(let value):
                _ = self.bind(value)
            case .number(let value):
                self.bind(value)
            case .date(let value):
                self.bind(value)
            case .data(let value):
                self.bind(value)
            case .asset(let value):
                self.bind(value)
            }
        }
        
        return self
    }
}

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
    
    /// Create a query object.
    func query(_ queryString: String) throws -> SQLiteQuery {
        try .init(queryString: queryString, connection: connection, config: config)
    }
    
    /// Execute a query on the database connection.
    func query(_ queryString: String, bindings: [any _AnyCryoColumnValue]) throws {
        config?.log?(.debug, "[SQLite3Connection] query \(queryString), bindings \(bindings.map { "\($0)" })")
        
        var queryStatement: OpaquePointer?
        
        let prepareStatus = sqlite3_prepare_v3(connection, queryString, -1, 0, &queryStatement, nil)
        guard prepareStatus == SQLITE_OK else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryCompilationFailed(query: queryString, status: prepareStatus, message: message)
        }
        
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        for i in 0..<bindings.count {
            try self.bindValue(queryStatement!, value: bindings[i], index: Int32(i + 1))
        }
        
        let executeStatus = sqlite3_step(queryStatement)
        guard executeStatus == SQLITE_DONE else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryExecutionFailed(query: queryString, status: executeStatus, message: message)
        }
    }
    
    /// Bind a value in the given query.
    fileprivate func bindValue(_ statement: OpaquePointer, value: _AnyCryoColumnValue, index: Int32) throws {
        switch value {
        case let value as CryoColumnIntValue:
            sqlite3_bind_int(statement, index, Int32(value.integerValue))
        case let value as CryoColumnDoubleValue:
            sqlite3_bind_double(statement, index, value.doubleValue)
        case let url as URL:
            _ = url.absoluteString.utf8CString.withUnsafeBufferPointer { buffer in
                sqlite3_bind_text(statement, index, buffer.baseAddress, -1, SQLITE_TRANSIENT)
            }
        case let value as CryoColumnStringValue:
            _ = value.stringValue.utf8CString.withUnsafeBufferPointer { buffer in
                sqlite3_bind_text(statement, index, buffer.baseAddress, -1, SQLITE_TRANSIENT)
            }
        case let value as CryoColumnDateValue:
            let dateString = ISO8601DateFormatter().string(from: value.dateValue)
            _ = dateString.utf8CString.withUnsafeBufferPointer { buffer in
                sqlite3_bind_text(statement, index, buffer.baseAddress, -1, SQLITE_TRANSIENT)
            }
        case let value as CryoColumnDataValue:
            let data = try value.dataValue
            _ = data.withUnsafeBytes { (bytes: UnsafeRawBufferPointer) in
                sqlite3_bind_blob(statement, index, bytes.baseAddress, Int32(bytes.count), nil)
            }
        default:
            let data = try JSONEncoder().encode(value)
            _ = data.withUnsafeBytes { (bytes: UnsafeRawBufferPointer) in
                sqlite3_bind_blob(statement, index, bytes.baseAddress, Int32(bytes.count), nil)
            }
        }
    }
    
    /// Execute a query on the database connection.
    func query(_ queryString: String,
               bindings: [any _AnyCryoColumnValue],
               columns: [(String, CryoColumnType)]) throws -> [[any _AnyCryoColumnValue]] {
        config?.log?(.debug, "[SQLite3Connection] query \(queryString), bindings \(bindings.map { "\($0)" }), columns \(columns.map { $0.0 })")
        
        var queryStatement: OpaquePointer?
        
        let prepareStatus = sqlite3_prepare_v3(connection, queryString, -1, 0, &queryStatement, nil)
        guard prepareStatus == SQLITE_OK else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryCompilationFailed(query: queryString, status: prepareStatus, message: message)
        }
        
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        for i in 0..<bindings.count {
            try self.bindValue(queryStatement!, value: bindings[i], index: Int32(i + 1))
        }
        
        var executeStatus = sqlite3_step(queryStatement)
        var result = [[any _AnyCryoColumnValue]]()
        
        while executeStatus == SQLITE_ROW {
            var row = [any _AnyCryoColumnValue]()
            
            for i in 0..<columns.count {
                let value = try self.columnValue(queryStatement!, columnName: columns[i].0, type: columns[i].1, index: Int32(i))
                row.append(value)
            }
            
            result.append(row)
            executeStatus = sqlite3_step(queryStatement)
        }
        
        guard executeStatus == SQLITE_DONE else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryExecutionFailed(query: queryString, status: executeStatus, message: message)
        }
        
        return result
    }
    
    /// Get a result value from the given query.
    private func columnValue(_ statement: OpaquePointer, columnName: String,
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

public final actor SQLiteAdaptor {
    /// The database connection object.
    fileprivate let db: SQLite3Connection
    
    /// The database URL.
    let databaseUrl: URL
    
    /// Cache for schemas.
    var schemas: [ObjectIdentifier: CryoSchema]
    
    /// The cryo config.
    let config: CryoConfig?
    
    /// The created tables.
    var createdTables = Set<String>()
    
    /// Create an SQLite adaptor.
    public init(databaseUrl: URL, config: CryoConfig? = nil) throws {
        self.schemas = [:]
        self.databaseUrl = databaseUrl
        self.config = config
        self.db = try .init(databaseUrl: databaseUrl, config: config)
    }
}

extension SQLiteAdaptor {
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
    
    /// Find or create a schema.
    func schema<Model: CryoModel>(for model: Model.Type) throws -> CryoSchema {
        let schemaKey = ObjectIdentifier(Model.self)
        if let schema = self.schemas[schemaKey] {
            return schema
        }
        
        let schema = Model.schema
        self.schemas[schemaKey] = schema
        
        try self.createTable(for: model)
        return schema
    }
    
    /// Create a table if it does not exist yet.
    public func createTable<Model: CryoModel>(for model: Model.Type) throws {
        guard createdTables.insert(Model.tableName).inserted else {
            return
        }
        
        let query = try self.createTableQuery(for: model)
        try db.query(query, bindings: [])
    }
    
    /// Create a generic query.
    public func query(_ queryString: String) throws -> SQLiteQuery {
        try db.query(queryString)
    }
    
    // MARK: Create table
    
    /// Build the query for creating a table for a given model.
    func createTableQuery<Model: CryoModel>(for modelType: Model.Type) throws -> String {
        let schema = try self.schema(for: modelType)
        var columns = ""
        
        for columnDetails in schema {
            columns += ",\n    \(columnDetails.columnName) \(Self.sqliteTypeName(for: columnDetails.type))"
        }
        
        return """
CREATE TABLE IF NOT EXISTS \(Model.tableName)(
    _cryo_key TEXT NOT NULL UNIQUE,
    _cryo_created TEXT NOT NULL,
    _cryo_modified TEXT NOT NULL\(columns)
);
"""
    }
    
    // MARK: Insertion
    
    /// Build the query string for an insertion query.
    func createInsertQuery<Model: CryoModel>(for value: Model) throws -> String {
        let schema = try self.schema(for: type(of: value))
        let columns: [String] = schema.map { $0.columnName }
        
        return """
INSERT OR REPLACE INTO \(Model.tableName)(_cryo_key,_cryo_created,_cryo_modified,\(columns.joined(separator: ","))) VALUES (?,?,?,\(columns.map { _ in "?" }.joined(separator: ",")));
"""
    }
    
    /// Get the values for an insertion query.
    func getInsertBindings<Key: CryoKey, Model: CryoModel>(for key: Key, value: Model) throws -> [any _AnyCryoColumnValue] {
        let schema = try self.schema(for: type(of: value))
        let now = ISO8601DateFormatter().string(from: .now)
        
        var bindings: [any _AnyCryoColumnValue] = [key.id, now, now]
        bindings.append(contentsOf: schema.map { $0.getValue(value) })
        
        return bindings
    }
    
    // MARK: Deletion
    
    /// Build the query string for a deletion query.
    func createDeleteQuery<Model: CryoModel>(for key: Model.Type) throws -> String {
        "DELETE FROM \(Model.tableName) WHERE _cryo_key == ?;"
    }
    
    /// Get the values for a deletion query.
    func getDeleteBindings<Key: CryoKey>(for key: Key) throws -> [any _AnyCryoColumnValue] {
        [key.id]
    }
    
    /// Build the query string for a deletion query.
    func createDeleteAllQuery<Model: CryoModel>(for key: Model.Type) throws -> String {
        "DELETE FROM \(Model.tableName);"
    }
    
    // MARK: Update
    
    /// Build the query string for an update query.
    func createUpdateQuery<Model: CryoModel>(for value: Model) throws -> String {
        let schema = try self.schema(for: type(of: value))
        let columns: [String] = schema.map { $0.columnName }
        
        return "UPDATE \(Model.tableName) SET _cryo_modified = ?, \(columns.map { "\($0) = ?" }.joined(separator: ", ")) WHERE _cryo_key == ?;"
    }
    
    /// Get the values for an update query.
    func getUpdateBindings<Key: CryoKey, Model: CryoModel>(for key: Key, value: Model)
        throws -> [any _AnyCryoColumnValue]
    {
        let schema = try self.schema(for: type(of: value))
        
        var bindings: [any _AnyCryoColumnValue] = [ISO8601DateFormatter().string(from: .now)]
        bindings.append(contentsOf: schema.map { $0.getValue(value) })
        bindings.append(key.id)
        
        return bindings
    }
    
    // MARK: Select
    
    /// Build a selection query string.
    func createSelectAllQuery<Model: CryoModel>(for value: Model.Type) throws -> String {
        let schema = try self.schema(for: value)
        let columns: [String] = schema.map { $0.columnName }
        
        return "SELECT \(columns.joined(separator: ",")) FROM \(Model.tableName);"
    }
    
    /// Build a selection query string.
    func createSelectByIdQuery<Model: CryoModel>(for value: Model.Type) throws -> String {
        let schema = try self.schema(for: value)
        let columns: [String] = schema.map { $0.columnName }
        
        return "SELECT \(columns.joined(separator: ",")) FROM \(Model.tableName) WHERE _cryo_key == ? LIMIT 1;"
    }
    
    /// Get the values for a selection query.
    func getSelectByIdBindings<Key: CryoKey>(for key: Key) throws -> [any _AnyCryoColumnValue] {
        [key.id]
    }
    
    // MARK: Attach
    
    /// Build a query string to attach another database file.
    func createAttachQuery(databaseUrl: URL, name: String) throws -> String {
        "ATTACH DATABASE \(databaseUrl.absoluteString) AS \(name);"
    }
    
    /// Build a query string to detach another database file.
    func createDetachQuery(name: String) throws -> String {
        "DETACH \(name);"
    }
    
    // MARK: Transactions
    
    /// Build the query for a transaction start.
    func createBeginTransactionQuery() throws -> String {
        "BEGIN;"
    }
    
    /// Build the query for a transaction commit.
    func createCommitTransactionQuery() throws -> String {
        "COMMIT;"
    }
}

extension SQLiteAdaptor {
    /// Execute operations in a transaction.
    public func transaction(_ operations: () async throws -> Void) async throws {
        try db.query(self.createBeginTransactionQuery(), bindings: [])
        defer {
            do {
                try db.query(self.createCommitTransactionQuery(), bindings: [])
            }
            catch { }
        }
        
        try await operations()
    }
    
    /// Execute operations with another attached database.
    public func withAttachedDatabase(databaseUrl: URL, _ operations: (String) async throws -> Void) async throws {
        let databaseName = "\(UUID().uuidString.prefix(10))".replacingOccurrences(of: "-", with: "_")
        
        try db.query(self.createAttachQuery(databaseUrl: databaseUrl, name: databaseName), bindings: [])
        defer {
            do {
                try db.query(self.createDetachQuery(name: databaseName), bindings: [])
            }
            catch { }
        }
        
        try await operations(databaseName)
    }
}

extension SQLiteAdaptor: CryoIndexingAdaptor {
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws {
        guard let model = value as? CryoModel else {
            throw CryoError.cannotPersistValue(valueType: Key.Value.self, adaptorType: SQLiteAdaptor.self)
        }
        
        let query = try createInsertQuery(for: model)
        let bindings = try getInsertBindings(for: key, value: model)
        
        try db.query(query, bindings: bindings)
    }
    
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value? {
        try self.loadSynchronously(with: key)
    }
    
    public func loadSynchronously<Key: CryoKey>(with key: Key) throws -> Key.Value? {
        guard let model = Key.Value.self as? CryoModel.Type else {
            throw CryoError.cannotPersistValue(valueType: Key.Value.self, adaptorType: SQLiteAdaptor.self)
        }
        
        let schema = try self.schema(for: model)
        let query = try self.createSelectByIdQuery(for: model)
        let bindings = try self.getSelectByIdBindings(for: key)
        
        let rows = try db.query(query, bindings: bindings, columns: schema.map { ($0.columnName, $0.type) })
        guard let firstRow = rows.first else {
            return nil
        }
        
        var data = [String: _AnyCryoColumnValue]()
        for i in 0..<schema.count {
            data[schema[i].columnName] = firstRow[i]
        }
        
        return try model.init(from: CryoModelDecoder(data: data)) as? Key.Value
    }
    
    public func loadAll<Record: CryoModel>(of type: Record.Type) async throws -> [Record]? {
        let model = Record.self
        let schema = try self.schema(for: model)
        let query = try self.createSelectAllQuery(for: model)
        
        var values = [Record]()
        
        let rows = try db.query(query, bindings: [], columns: schema.map { ($0.columnName, $0.type) })
        for row in rows {
            var data = [String: _AnyCryoColumnValue]()
            for i in 0..<schema.count {
                data[schema[i].columnName] = row[i]
            }
            
            values.append(try .init(from: CryoModelDecoder(data: data)))
        }
        
        return values
    }
    
    public func loadAllBatched<Record: CryoModel>(of type: Record.Type, receiveBatch: ([Record]) -> Bool) async throws {
        _ = receiveBatch(try await self.loadAll(of: type) ?? [])
    }
    
    public func remove<Key: CryoKey>(with key: Key) async throws {
        guard let model = Key.Value.self as? CryoModel.Type else {
            throw CryoError.cannotPersistValue(valueType: Key.Value.self, adaptorType: SQLiteAdaptor.self)
        }
        
        let query = try createDeleteQuery(for: model)
        let bindings = try getDeleteBindings(for: key)
        
        try db.query(query, bindings: bindings)
    }
    
    public func removeAll() async throws {
        try FileManager.default.removeItem(at: self.databaseUrl)
    }
    
    public func removeAll<Record: CryoModel>(of type: Record.Type) async throws {
        let query = try createDeleteAllQuery(for: Record.self)
        try db.query(query, bindings: [])
    }
}

extension SQLiteAdaptor: CryoDatabaseAdaptor {
    public func execute(operation: DatabaseOperation) async throws {
        let query: SQLiteQuery
        switch operation.type {
        case .insert:
            let columnNames = operation.data.map { $0.columnName }
            query = try self.query("""
INSERT OR REPLACE INTO \(operation.tableName)(_cryo_key,_cryo_created,_cryo_modified,\(columnNames.joined(separator: ","))) VALUES (?,?,?,\(columnNames.map { _ in "?" }.joined(separator: ",")));
""")
            .bind(operation.rowId)
            .bind(Date.now)
            .bind(Date.now)
            .bind(operation.data)
        case .update:
            let columnNames = operation.data.map { $0.columnName }
            query = try self.query("""
UPDATE \(operation.tableName) SET _cryo_modified = ?, \(columnNames.map { "\($0) = ?" }.joined(separator: ", ")) WHERE _cryo_key == ?;
""")
            .bind(Date.now)
            .bind(operation.data)
            .bind(operation.rowId)
        case .delete:
            if operation.tableName.isEmpty {
                try await self.removeAll()
                return
            }
            else if operation.rowId.isEmpty {
                query = try self.query("DELETE FROM \(operation.tableName);")
            }
            else {
                query = try self.query("DELETE FROM \(operation.tableName) WHERE _cryo_key == ?;")
                    .bind(operation.rowId)
            }
        }
        
        try query.execute()
    }
    
    public nonisolated var isAvailable: Bool { true }
    
    public func ensureAvailability() async throws {
        
    }
    
    public nonisolated func observeAvailabilityChanges(_ callback: @escaping (Bool) -> Void) {
        
    }
}
