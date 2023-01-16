
import Foundation

// MARK: CryoAdaptor

public protocol CryoAdaptor {
    /// Persist the given value for a key.
    func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws
    
    /// - returns: The value previously persisted for `key`, or nil if none exists.
    func load<Key: CryoKey>(with key: Key) async throws -> Key.Value?
    
    /// A synchronous version of the load method. Not all adaptors support synchronous loading, so use
    /// of this method should be avoided if possible.
    /// - returns: The value previously persisted for `key`, or nil if none exists.
    func loadSynchronously<Key: CryoKey>(with key: Key) throws -> Key.Value?
    
    /// Load all values of the given Key type. Not all adaptors support this operation.
    func loadAll<Key: CryoKey>(with key: Key.Type) async throws -> [Key.Value]?
    
    /// Load all values of the given Key type. Not all adaptors support this operation.
    func loadAllBatched<Key: CryoKey>(with key: Key.Type, receiveBatch: ([Key.Value]) -> Bool) async throws -> Bool
    
    /// Remove the given value for a key.
    func remove<Key: CryoKey>(with key: Key) async throws
    
    /// Remove the values for all keys associated with this adaptor.
    func removeAll() async throws
    
    /// Synchronize the changes of the adaptor with a backend, if applicable.
    func synchronize()
}

extension CryoAdaptor {
    /// - returns: The value previously persisted for `key`, or `defaultValue` if none exists.
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
    
    /// Remove the given value for a key.
    public func remove<Key: CryoKey>(with key: Key) async throws {
        try await persist(nil, for: key)
    }
    
    /// - returns: The value previously persisted for `key`, or nil if none exists.
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value? {
        try self.loadSynchronously(with: key)
    }
    
    /// - returns: The value previously persisted for `key`, or nil if none exists.
    public func loadSynchronously<Key: CryoKey>(with key: Key) throws -> Key.Value? {
        fatalError("adaptor \(Self.self) does not support synchronous loading")
    }
    
    /// Load all values of the given Key type. Not all adaptors support this operation.
    public func loadAll<Key: CryoKey>(with key: Key.Type) async throws -> [Key.Value]? {
        var values = [Key.Value]()
        
        let isSupported = try await self.loadAllBatched(with: Key.self) { nextBatch in
            values.append(contentsOf: nextBatch)
            return true
        }
        
        guard isSupported else { return nil }
        return values
    }
    
    /// Load all values of the given Key type. Not all adaptors support this operation.
    public func loadAllBatched<Key: CryoKey>(with key: Key.Type, receiveBatch: ([Key.Value]) -> Bool) async throws -> Bool { false }
    
    /// Synchronize the changes of the adaptor with a backend, if applicable.
    public func synchronize() { }
}
