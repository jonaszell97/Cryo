
import Foundation
import SQLite3

fileprivate let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

fileprivate final class SQLite3Connection {
    /// The pointer to the database connection object.
    let connection: OpaquePointer
    
    /// Create a database connection.
    init(databaseUrl: URL) throws {
        var connection: OpaquePointer?
        let status = sqlite3_open(databaseUrl.absoluteString, &connection)
        
        guard status == SQLITE_OK, let connection else {
            throw CryoError.databaseConnectionFailed(dbName: databaseUrl.absoluteString, status: status)
        }
        
        self.connection = connection
    }
    
    /// Close the connection.
    deinit {
        sqlite3_close(connection)
    }
    
    /// Execute a query on the database connection.
    func query(_ queryString: String, bindings: [any _AnyCryoColumnValue]) throws {
        var queryStatement: OpaquePointer?
        
        let prepareStatus = sqlite3_prepare_v3(connection, queryString, -1, 0, &queryStatement, nil)
        guard prepareStatus == SQLITE_OK else {
            throw CryoError.queryCompilationFailed(query: queryString, status: prepareStatus)
        }
        
        defer {
            sqlite3_finalize(queryStatement)
        }
        
        for i in 0..<bindings.count {
            try self.bindValue(queryStatement!, value: bindings[i], index: Int32(i + 1))
        }
        
        let executeStatus = sqlite3_step(queryStatement)
        guard executeStatus == SQLITE_DONE else {
            throw CryoError.queryExecutionFailed(query: queryString, status: executeStatus)
        }
    }
    
    /// Bind a value in the given query.
    private func bindValue(_ statement: OpaquePointer, value: _AnyCryoColumnValue, index: Int32) throws {
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
        var queryStatement: OpaquePointer?
        
        let prepareStatus = sqlite3_prepare_v3(connection, queryString, -1, 0, &queryStatement, nil)
        guard prepareStatus == SQLITE_OK else {
            throw CryoError.queryCompilationFailed(query: queryString, status: prepareStatus)
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
                let value = try self.columnValue(queryStatement!, columnName: columns[i].0, type: columns[i].1, index: Int32(i + 1))
                row.append(value)
            }
            
            result.append(row)
            executeStatus = sqlite3_step(queryStatement)
        }
        
        guard executeStatus == SQLITE_DONE else {
            throw CryoError.queryExecutionFailed(query: queryString, status: executeStatus)
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
                throw CryoError.queryDecodeFailed(column: columnName)
            }
            
            return String(cString: absoluteString)
        case .date:
            guard let dateString = sqlite3_column_text(statement, index) else {
                throw CryoError.queryDecodeFailed(column: columnName)
            }
            guard let date = ISO8601DateFormatter().date(from: String(cString: dateString)) else {
                throw CryoError.queryDecodeFailed(column: columnName)
            }
            
            return date
        case .data:
            let byteCount = sqlite3_column_bytes(statement, index)
            guard let blob = sqlite3_column_blob(statement, index) else {
                throw CryoError.queryDecodeFailed(column: columnName)
            }
            
            return Data(bytes: blob, count: Int(byteCount))
        case .bool:
            return sqlite3_column_int(statement, index) != 0
        case .asset:
            fatalError("not supported in SQLiteAdaptor")
        }
    }
}

public final class SQLiteAdaptor {
    /// The database connection object.
    fileprivate let db: SQLite3Connection
    
    /// The database URL.
    let databaseUrl: URL
    
    /// Cache for schemas.
    var schemas: [ObjectIdentifier: CryoSchema]
    
    /// Create an SQLite adaptor.
    public init(databaseUrl: URL) throws {
        self.schemas = [:]
        self.databaseUrl = databaseUrl
        self.db = try .init(databaseUrl: databaseUrl)
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
        
        let query = try self.createTableQuery(for: model)
        try db.query(query, bindings: [])
        
        return schema
    }
    
    // MARK: Create table
    
    /// Build the query for creating a table for a given model.
    func createTableQuery<Model: CryoModel>(for modelType: Model.Type) throws -> String {
        let schema = try self.schema(for: modelType)
        var columns = "_cryo_key TEXT NOT NULL UNIQUE"
        
        for columnDetails in schema {
            columns += ",\n    \(columnDetails.columnName) \(Self.sqliteTypeName(for: columnDetails.type))"
        }
        
        return """
CREATE TABLE IF NOT EXISTS \(Model.tableName)(
    \(columns)
);
"""
    }
  
    // MARK: Insertion
    
    /// Build the query string for an insertion query.
    func createInsertQuery<Model: CryoModel>(for value: Model) throws -> String {
        let schema = try self.schema(for: type(of: value))
        let columns: [String] = schema.map { $0.columnName }
        
        return """
INSERT INTO \(Model.tableName)(_cryo_key,\(columns.joined(separator: ","))) VALUES (?,\(columns.map { _ in "?" }.joined(separator: ",")));
"""
    }
    
    /// Get the values for an insertion query.
    func getInsertBindings<Key: CryoKey, Model: CryoModel>(for key: Key, value: Model) throws -> [any _AnyCryoColumnValue] {
        let schema = try self.schema(for: type(of: value))
        var bindings: [any _AnyCryoColumnValue] = [key.id]
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
        
        return "UPDATE \(Model.tableName) SET \(columns.map { "\($0) = ?" }.joined(separator: ", ")) WHERE _cryo_key == ?;"
    }
    
    /// Get the values for an update query.
    func getUpdateBindings<Key: CryoKey, Model: CryoModel>(for key: Key, value: Model)
        throws -> [any _AnyCryoColumnValue]
    {
        let schema = try self.schema(for: type(of: value))
        
        var bindings = schema.map { $0.getValue(value) }
        bindings.append(key.id)
        
        return bindings
    }
    
    // MARK: Select
    
    /// Build a selection query string.
    func createSelectAllQuery<Model: CryoModel>(for value: Model.Type) throws -> String {
        "SELECT * FROM \(Model.tableName);"
    }
    
    /// Build a selection query string.
    func createSelectByIdQuery<Model: CryoModel>(for value: Model.Type) throws -> String {
        "SELECT * FROM \(Model.tableName) WHERE _cryo_key == ? LIMIT 1;"
    }
    
    /// Get the values for a selection query.
    func getSelectByIdBindings<Key: CryoKey>(for key: Key) throws -> [any _AnyCryoColumnValue] {
        [key.id]
    }
}

extension SQLiteAdaptor: CryoAdaptor, CryoSynchronousAdaptor, CryoIndexingAdaptor {
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws {
        guard let model = value as? CryoModel else {
            throw CryoError.cannotPersistValue(valueType: Key.Value.self, adaptorType: SQLiteAdaptor.self)
        }
        
        let query: String
        let bindings: [any _AnyCryoColumnValue]
        
        if try await load(with: key) != nil {
            query = try createUpdateQuery(for: model)
            bindings = try getUpdateBindings(for: key, value: model)
        }
        else {
            query = try createInsertQuery(for: model)
            bindings = try getInsertBindings(for: key, value: model)
        }
        
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
    
    public func loadAll<Key: CryoKey>(with key: Key.Type) async throws -> [Key.Value]? where Key.Value: CryoModel {
        let model = Key.Value.self
        let schema = try self.schema(for: model)
        let query = try self.createSelectAllQuery(for: model)
        
        var values = [Key.Value]()
        
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
    
    public func loadAllBatched<Key: CryoKey>(with key: Key.Type, receiveBatch: ([Key.Value]) -> Bool) async throws where Key.Value: CryoModel {
        _ = receiveBatch(try await self.loadAll(with: key) ?? [])
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
    
    public func removeAll<Key: CryoKey>(with key: Key.Type) async throws where Key.Value: CryoModel {
        let query = try createDeleteAllQuery(for: Key.Value.self)
        try db.query(query, bindings: [])
    }
}
