
import Foundation

/// Common type for errors thrown in `Cryo`.
public enum CryoError: Error {
    /// A value cannot be persisted with the given adaptor.
    case cannotPersistValue(valueType: Any.Type, adaptorType: Any.Type)
    
    // MARK: CloudKit
    
    /// Failed to connect to a database.
    case databaseConnectionFailed(dbName: String, status: Int32)
    
    /// The storage backend is not available.
    case backendNotAvailable
    
    // MARK: SQLite
    
    /// Failed to compile an SQL query.
    case queryCompilationFailed(query: String, status: Int32, message: String?)
    
    /// Failed to execute an SQL query.
    case queryExecutionFailed(query: String, status: Int32, message: String?)
    
    /// Failed to read a column value.
    case queryDecodeFailed(column: String, message: String?)
}
