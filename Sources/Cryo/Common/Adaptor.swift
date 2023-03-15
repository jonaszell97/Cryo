
import Foundation

/// Provides a unified interface for heterogeneous persistence backends.
///
/// `CryoAdaptor` implementations are responsible for persisting and loading data in `Cryo`.
/// Data is persisted using the ``CryoAdaptor/persist(_:for:)`` method, which receives a persistable value
/// as well as a key. Keys must conform to the ``CryoKey`` protocol and uniquely identify a persistable resource.
///
/// All codable types can be persisted with `CryoAdaptor`, although there may be optimized implementations for some
/// known types.
///
/// ```swift
/// // Persist a value using an adaptor
/// try await adaptor.persist("Hello, World", myKey)
///
/// // Retrieve the value
/// print(try await adaptor.load(with: myKey)!)
/// ```
public protocol CryoAdaptor {
    /// Persist the given value for a key.
    ///
    /// - Parameters:
    ///   - value: The value to persist. If this parameter is `nil`, the value for the given key is removed.
    ///   - key: The key that uniquely identifies the persisted value.
    func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws
    
    /// Load a persisted value for a key.
    ///
    /// - Parameter key: The key that uniquely identifies the persisted value.
    /// - Returns: The value previously persisted for `key`, or nil if none exists.
    func load<Key: CryoKey>(with key: Key) async throws -> Key.Value?
    
    /// Remove the given value for a key.
    ///
    /// - Parameter key: The key that uniquely identifies the value to remove.
    func remove<Key: CryoKey>(with key: Key) async throws
    
    /// Remove the values for all keys associated with this adaptor.
    ///
    /// - Warning: This is a destructive operation. Be sure to check whether you really want
    /// to delete all data before calling it.
    func removeAll() async throws
    
    /// Synchronize the changes of the adaptor with a backend, if applicable.
    ///
    /// - Note: Not all adaptors support this operation. If not supported, it is a no-op.
    func synchronize()
}

extension CryoAdaptor {
    /// Load a value for a key or place a default value if none exists.
    ///
    /// - Parameters:
    ///   - key: The key that uniquely identifies the persisted value.
    ///   - defaultValue: The default value to persist and return if no value exists.
    /// - Returns: The value previously persisted for `key`, or `defaultValue` if none exists.
    public func load<Key: CryoKey>(with key: Key, defaultValue: @autoclosure () -> Key.Value) async throws
        -> Key.Value
    {
        guard let value = try await self.load(with: key) else {
            let value = defaultValue()
            try await self.persist(value, for: key)
            
            return value
        }
        
        return value
    }
    
    public func remove<Key: CryoKey>(with key: Key) async throws {
        try await persist(nil, for: key)
    }
    
    public func synchronize() { }
}

public protocol CryoSynchronousAdaptor: CryoAdaptor {
    /// A synchronous version of ``CryoAdaptor/load(with:)-25w6c``.
    ///
    /// - Parameter key: The key that uniquely identifies the persisted value.
    /// - Returns: The value previously persisted for `key`, or nil if none exists.
    func loadSynchronously<Key: CryoKey>(with key: Key) throws -> Key.Value?
}

extension CryoSynchronousAdaptor {
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value? {
        try self.loadSynchronously(with: key)
    }
}

public protocol CryoIndexingAdaptor {
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
    
    /// Remove the given value for a key.
    ///
    /// - Parameter key: The key that uniquely identifies the value to remove.
    func remove<Key: CryoKey>(with key: Key) async throws
        where Key.Value: CryoModel
    
    /// Load all values of the given `Key` type. Not all adaptors support this operation.
    ///
    /// - Parameter key: The Key type of which all values should be loaded.
    /// - Returns: All values of the given key, or `nil` if the adaptor does not support this operation.
    func loadAll<Record: CryoModel>(of type: Record.Type) async throws -> [Record]?
    
    /// Load all values of the given `Key` type in batches. Not all adaptors support this operation.
    ///
    /// - Parameters:
    ///   - key: The Key type of which all values should be loaded.
    ///   - receiveBatch: Closure that is invoked whenever a new batch of values is fetched. If this closure
    ///   returns `false`, no more batches will be fetched.
    /// - Returns: `true` if batched loading is supported.
    func loadAllBatched<Record: CryoModel>(of type: Record.Type, receiveBatch: ([Record]) -> Bool) async throws
    
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

extension CryoIndexingAdaptor {
    public func loadAll<Record: CryoModel>(of type: Record.Type) async throws -> [Record]? {
        var values = [Record]()
        try await self.loadAllBatched(of: Record.self) { nextBatch in
            values.append(contentsOf: nextBatch)
            return true
        }
        
        return values
    }
    
    public func remove<Key: CryoKey>(with key: Key) async throws
        where Key.Value: CryoModel
    {
        try await persist(nil, for: key)
    }
}

public protocol CryoObservableAdaptor {
    /// The change data type.
    associatedtype ChangeData = Void
    
    /// The change observer identifier type.
    associatedtype ObserverID = Void
    
    /// Install a listener for external changes.
    func observeChanges(_ callback: @escaping (ChangeData) -> Void) -> ObserverID
    
    /// Remove a change observer.
    func removeObserver(withId id: ObserverID)
}
