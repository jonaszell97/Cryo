
import Foundation

// MARK: CryoAdaptor

public protocol CryoAdaptor {
    /// Persist the given value for a key.
    func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws
        where Key.Value: CryoPersistable
    
    /// - returns: The value previously persisted for `key`, or nil if none exists.
    func load<Key: CryoKey>(with key: Key) async throws -> Key.Value?
        where Key.Value: CryoPersistable
    
    /// Remove the values for all keys associated with this adaptor.
    func removeAll() async throws
    
    /// Synchronize the changes of the adaptor with a backend, if applicable.
    func synchronize()
}

extension CryoAdaptor {
    /// - returns: The value previously persisted for `key`, or `defaultValue` if none exists.
    public func load<Key: CryoKey>(with key: Key, defaultValue: @autoclosure () -> Key.Value) async throws
        -> Key.Value where Key.Value: CryoPersistable
    {
        guard let value = try await self.load(with: key) else {
            return defaultValue()
        }
        
        return value
    }
    
    /// Remove the given value for a key.
    public func remove<Key: CryoKey>(with key: Key) async throws where Key.Value: CryoPersistable {
        try await persist(nil, for: key)
    }
    
    /// Synchronize the changes of the adaptor with a backend, if applicable.
    public func synchronize() { }
}
