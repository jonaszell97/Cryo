
import Foundation

/// Common type for errors thrown in `Cryo`.
public enum CryoError: Error {
    /// A value cannot be persisted with the given adaptor.
    case cannotPersistValue(valueType: Any.Type, adaptorType: Any.Type)
    
    /// Failed to connect to a database.
    case databaseConnectionFailed(dbName: String, status: Int32)
    
    /// Failed to compile an SQL query.
    case queryCompilationFailed(query: String, status: Int32)
    
    /// Failed to execute an SQL query.
    case queryExecutionFailed(query: String, status: Int32)
    
    /// Failed to read a column value.
    case queryDecodeFailed(column: String)
}
