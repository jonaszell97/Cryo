
import Foundation
import SQLite3

public protocol CryoSQLiteValue {
    /// The SQLite type name for this value.
    var typeName: String { get }
    
    /// Bind this value to a query statement.
    func bind(to queryStatement: OpaquePointer, index: Int32) throws
    
    /// Get a result value of this type from the given query.
    static func columnValue(of queryStatement: OpaquePointer, connection: OpaquePointer,
                            columnName: String, index: Int32) throws -> Self
}

// MARK: Integer conformance to CryoSQLiteValue

extension Int: CryoSQLiteValue {
    public var typeName: String {
        return "INTEGER"
    }
    
    public func bind(to queryStatement: OpaquePointer, index: Int32) {
        sqlite3_bind_int64(queryStatement, index, Int64(self))
    }
    
    public static func columnValue(of queryStatement: OpaquePointer, connection: OpaquePointer,
                                   columnName: String, index: Int32) -> Self {
        Self(sqlite3_column_int64(queryStatement, index))
    }
}

extension UInt: CryoSQLiteValue {
    public var typeName: String {
        return "INTEGER"
    }
    
    public func bind(to queryStatement: OpaquePointer, index: Int32) {
        sqlite3_bind_int64(queryStatement, index, Int64(bitPattern: UInt64(self)))
    }
    
    public static func columnValue(of queryStatement: OpaquePointer, connection: OpaquePointer,
                                   columnName: String, index: Int32) -> Self {
        Self(sqlite3_column_int64(queryStatement, index))
    }
}

// MARK: Double conformance to CryoSQLiteValue

extension Double: CryoSQLiteValue {
    public var typeName: String {
        return "NUMERIC"
    }
    
    public func bind(to queryStatement: OpaquePointer, index: Int32) {
        sqlite3_bind_double(queryStatement, index, self)
    }
    
    public static func columnValue(of queryStatement: OpaquePointer, connection: OpaquePointer,
                                   columnName: String, index: Int32) -> Self {
        sqlite3_column_double(queryStatement, index)
    }
}

// MARK: Bool conformance to CryoSQLiteValue

extension Bool: CryoSQLiteValue {
    public var typeName: String {
        return "INTEGER"
    }
    
    public func bind(to queryStatement: OpaquePointer, index: Int32) {
        sqlite3_bind_int(queryStatement, index, self ? 1 : 0)
    }
    
    public static func columnValue(of queryStatement: OpaquePointer, connection: OpaquePointer,
                                   columnName: String, index: Int32) -> Self {
        sqlite3_column_int(queryStatement, index) != 0
    }
}

// MARK: String conformance to CryoSQLiteValue

extension String: CryoSQLiteValue {
    public var typeName: String {
        return "TEXT"
    }
    
    public func bind(to queryStatement: OpaquePointer, index: Int32) {
        _ = self.utf8CString.withUnsafeBufferPointer { buffer in
            sqlite3_bind_text(queryStatement, index, buffer.baseAddress, -1, SQLiteAdaptor.SQLITE_TRANSIENT)
        }
    }
    
    public static func columnValue(of queryStatement: OpaquePointer, connection: OpaquePointer,
                                   columnName: String, index: Int32) throws -> Self {
        guard let absoluteString = sqlite3_column_text(queryStatement, index) else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryDecodeFailed(column: columnName, message: message)
        }
        
        return String(cString: absoluteString)
    }
}

// MARK: Data conformance to CryoSQLiteValue

extension Data: CryoSQLiteValue {
    public var typeName: String {
        return "BLOB"
    }
    
    public func bind(to queryStatement: OpaquePointer, index: Int32) {
        _ = self.withUnsafeBytes { (bytes: UnsafeRawBufferPointer) in
            sqlite3_bind_blob(queryStatement, index, bytes.baseAddress, Int32(bytes.count), nil)
        }
    }
    
    public static func columnValue(of queryStatement: OpaquePointer, connection: OpaquePointer,
                                   columnName: String, index: Int32) throws -> Self {
        let byteCount = sqlite3_column_bytes(queryStatement, index)
        guard let blob = sqlite3_column_blob(queryStatement, index) else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryDecodeFailed(column: columnName, message: message)
        }
        
        return Data(bytes: blob, count: Int(byteCount))
    }
}

// MARK: Date conformance to CryoSQLiteValue

extension Date: CryoSQLiteValue {
    public var typeName: String {
        return "TEXT"
    }
    
    public func bind(to queryStatement: OpaquePointer, index: Int32) {
        let stringValue = ISO8601DateFormatter().string(from: self)
        _ = stringValue.utf8CString.withUnsafeBufferPointer { buffer in
            sqlite3_bind_text(queryStatement, index, buffer.baseAddress, -1, SQLiteAdaptor.SQLITE_TRANSIENT)
        }
    }
    
    public static func columnValue(of queryStatement: OpaquePointer, connection: OpaquePointer,
                                   columnName: String, index: Int32) throws -> Self {
        guard
            let dateString = sqlite3_column_text(queryStatement, index),
            let date = ISO8601DateFormatter().date(from: String(cString: dateString))
        else {
            var message: String? = nil
            if let errorPointer = sqlite3_errmsg(connection) {
                message = String(cString: errorPointer)
            }
            
            throw CryoError.queryDecodeFailed(column: columnName, message: message)
        }
        
        return date
    }
}
