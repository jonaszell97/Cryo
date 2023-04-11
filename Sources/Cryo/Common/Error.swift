
import Foundation

/// Common type for errors thrown in `Cryo`.
public enum CryoError: Error {
    /// A value cannot be persisted with the given adaptor.
    case cannotPersistValue(valueType: Any.Type, adaptorType: Any.Type)
    
    // MARK: Databases
    
    /// Failed to connect to a database.
    case databaseConnectionFailed(dbName: String, status: Int32)
    
    /// The storage backend is not available.
    case backendNotAvailable
    
    /// A finalized query was attempted to be modified.
    case modifyingFinalizedQuery
    
    /// A query failed because of a duplicate key.
    case duplicateId(id: String)
    
    /// A schema for a table was not initialized.
    case schemaNotInitialized(tableName: String)
    
    /// A foreign key constraint failed.
    case foreignKeyConstraintFailed(tableName: String, message: String?)
    
    // MARK: SQLite
    
    /// Failed to compile an SQL query.
    case queryCompilationFailed(query: String, status: Int32, message: String?)
    
    /// Failed to execute an SQL query.
    case queryExecutionFailed(query: String, status: Int32, message: String?)
    
    /// Failed to read a column value.
    case queryDecodeFailed(column: String, message: String?)
}
