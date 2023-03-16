
import Foundation

/// Provides a unified interface for heterogeneous database backends.
///
/// Values stored by database adaptors must conform to the ``CryoModel`` protocol. For every such type,
/// this adaptor creates a CloudKit table whose name is given by the ``CryoModel/tableName-1tzy7`` property.
///
/// A column is created for every model property that is annotated with either ``CryoColumn`` or ``CryoAsset``.
///
/// - Note: This adaptor does not support synchronous loading via ``CryoSyncronousAdaptor/loadSynchronously(with:)``.
///
/// Take the following model definition as an example:
///
/// ```swift
/// struct Message: CryoModel {
///     @CryoColumn var content: String
///     @CryoColumn var created: Date
///     @CryoAsset var attachment
/// }
///
/// try await adaptor.persist(Message(content: "Hello", created: Date.now, attachment: /*...*/),
///                           with: CryoNamedKey(id: "1", for: Message.self))
/// try await adaptor.persist(Message(content: "Hi", created: Date.now, attachment: /*...*/),
///                           with: CryoNamedKey(id: "2", for: Message.self))
/// try await adaptor.persist(Message(content: "How are you?", created: Date.now, attachment: /*...*/),
///                           with: CryoNamedKey(id: "3", for: Message.self))
///
/// ```
///
/// Based on this definition, `CloudKitAdaptor` will create a table in CloudKIt named `Message`
/// with the following structure:
///
/// | ID  | content: `NSString` | created: `NSDate` | attachment: `NSURL` |
/// | ---- | ---------- | ---------- | -------------- |
/// | 1   | "Hello"  | YYYY-MM-DD | /... |
/// | 2   | "Hi"  | YYYY-MM-DD | /... |
/// | 3   | "How are you?"  | YYYY-MM-DD | /... |
public protocol CryoDatabaseAdaptor {
    // MARK: Operations
    
    /// Execute a database operation.
    ///
    /// - Parameter operation: The operation to execute.
    func execute(operation: DatabaseOperation) async throws
    
    // MARK: Queries
    
    /// Create a SELECT query.
    func select<Model: CryoModel>(from: Model.Type) async throws -> any CryoSelectQuery<Model>
    
    // MARK: Availability
    
    /// Check the availability of the database.
    var isAvailable: Bool { get }
    
    /// Ensure that the database is available and throw an error if it is not.
    ///
    /// - Throws: ``CryoError/backendNotAvailable`` if the database is not available.
    func ensureAvailability() async throws
    
    /// Register a listener for availability changes.
    ///
    /// - Parameter callback: The callback to invoke with the changed availability.
    func observeAvailabilityChanges(_ callback: @escaping (Bool) -> Void)
    
    // MARK: Utility
    
    /// Persist the given value for a key.
    ///
    /// - Parameters:
    ///   - value: The value to persist. If this parameter is `nil`, the value for the given key is removed.
    ///   - key: The key that uniquely identifies the persisted value.
    func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws
    where Key.Value: CryoModel
    
    /// Load a persisted value for a key.
    ///
    /// - Parameter key: The key that uniquely identifies the persisted value.
    /// - Returns: The value previously persisted for `key`, or nil if none exists.
    func load<Key: CryoKey>(with key: Key) async throws -> Key.Value?
    where Key.Value: CryoModel
    
    /// Load all values of the given `Key` type. Not all adaptors support this operation.
    ///
    /// - Parameter key: The Key type of which all values should be loaded.
    /// - Returns: All values of the given key, or `nil` if the adaptor does not support this operation.
    func loadAll<Record: CryoModel>(of type: Record.Type) async throws -> [Record]?
    
    /// Remove the given value for a key.
    ///
    /// - Parameter key: The key that uniquely identifies the value to remove.
    func remove<Key: CryoKey>(with key: Key) async throws
    where Key.Value: CryoModel
    
    /// Remove the values for all keys associated with this adaptor.
    ///
    /// - Warning: This is a destructive operation. Be sure to check whether you really want
    /// to delete all data before calling it.
    func removeAll<Record: CryoModel>(of type: Record.Type) async throws
    
    /// Remove the values for all keys associated with this adaptor.
    ///
    /// - Warning: This is a destructive operation. Be sure to check whether you really want
    /// to delete all data before calling it.
    func removeAll() async throws
}

extension CryoDatabaseAdaptor {
    public var isAvailable: Bool { true }
    public func ensureAvailability() { }
    public func observeAvailabilityChanges(_ callback: @escaping (Bool) -> Void) { }
    
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws
    where Key.Value: CryoModel
    {
        guard let value else {
            try await self.remove(with: key)
            return
        }
        
        let operation = DatabaseOperation.insert(tableName: Key.Value.tableName,
                                                 id: key.id,
                                                 data: try value.codableData)
        
        try await self.execute(operation: operation)
    }
    
    public func remove<Key: CryoKey>(with key: Key) async throws
    where Key.Value: CryoModel
    {
        let operation = DatabaseOperation.delete(tableName: Key.Value.tableName, id: key.id)
        try await self.execute(operation: operation)
    }
    
    public func removeAll<Record>(of type: Record.Type) async throws
    where Record: CryoModel
    {
        let operation = DatabaseOperation.delete(tableName: Record.tableName)
        try await self.execute(operation: operation)
    }
    
    public func removeAll() async throws {
        let operation = DatabaseOperation.deleteAll()
        try await self.execute(operation: operation)
    }
}
