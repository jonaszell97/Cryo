
import Foundation

// MARK: CryoKey

public protocol CryoKey {
    /// The value type stored by this key.
    associatedtype Value
    
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
