
import Foundation

// MARK: CryoKey

/// Uniquely identifies a persisted resource.
public protocol CryoKey {
    /// The value type stored by this key.
    associatedtype Value: Codable
    
    /// The unique identifier for the stored value.
    var id: String { get }
    
    /// An optional default value returned if no value with this key is found.
    var defaultValue: Value? { get }
    
    /// Whether values loaded from this key should be cached locally.
    var shouldCacheValues: Bool { get }
}

extension CryoKey {
    /// An optional default value returned if no value with this key is found.
    public var defaultValue: Value? { nil }
    
    /// Whether values loaded from this key should be cached locally.
    public var shouldCacheValues: Bool { true }
}

/// Basic implementation of ``CryoKey`` with a configurable value type and identifier.
public struct CryoNamedKey<Value: Codable>: CryoKey {
    /// The unique identifier for the stored value.
    public let id: String
    
    /// Create a named key.
    public init(id: String, for valueType: Value.Type) {
        self.id = id
    }
}
