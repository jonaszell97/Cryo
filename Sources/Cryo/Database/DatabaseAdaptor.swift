
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
    // MARK: Queries
    
    #if false
    
    /// Create a CREATE TABLE query.
    func createTable<Model: CryoModel>(for: Model.Type) async throws -> any CryoQuery<Void>
    
    /// Create a SELECT by ID query.
    func select<Model: CryoModel>(id: String?, from: Model.Type) async throws -> any CryoSelectQuery<Model>
    
    /// Create an INSERT query.
    func insert<Model: CryoModel>(id: String, _ value: Model, replace: Bool) async throws -> any CryoInsertQuery<Model>
    
    /// Create an UPDATE query.
    func update<Model: CryoModel>(id: String?, from: Model.Type) async throws -> any CryoUpdateQuery<Model>
    
    /// Create a DELETE query.
    func delete<Model: CryoModel>(id: String?, from: Model.Type) async throws -> any CryoDeleteQuery<Model>
    
    #endif
    
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
}

extension CryoDatabaseAdaptor {
    public var isAvailable: Bool { true }
    public func ensureAvailability() { }
    public func observeAvailabilityChanges(_ callback: @escaping (Bool) -> Void) { }
    
    #if false
    
    /// Create an INSERT query.
    public func insert<Model: CryoModel>(id: String, _ value: Model) async throws -> any CryoInsertQuery<Model> {
        try await self.insert(id: id, value, replace: true)
    }
    
    /// Create an UPDATE query.
    public func update<Model: CryoModel>(from: Model.Type) async throws -> any CryoUpdateQuery<Model> {
        try await self.update(id: nil, from: Model.self)
    }
    
    /// Create a SELECT query.
    public func select<Model: CryoModel>(from model: Model.Type) async throws -> any CryoSelectQuery<Model> {
        try await self.select(id: nil, from: model)
    }
    
    #endif
}
