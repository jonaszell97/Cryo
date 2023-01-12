
import Foundation

public typealias CryoModelDetails<Value> = [String: (CryoValue.ValueType, WritableKeyPath<Value, CryoValue>)]

/// Protocol for types that can be persisted using CloudKit.
public protocol CryoModel {
    /// The name for the table representing this model.
    static var tableName: String { get }
    
    /// - returns: The model for this instance.
    static var model: CryoModelDetails<Self> { get }
    
    /// Construct an empty value of this model type from the given data that can be reconstructed.
    init ()
}

/// Property wrapper for columns in a CryoModel.
@propertyWrapper public struct CryoColumn<Value: CryoPersistable> {
    /// The wrapped, persistable value.
    public var wrappedValue: Value
    
    /// Interface to a writable cryo value that proxies the wrapped value.
    public var projectedValue: CryoValue {
        get {
            try! wrappedValue.persistableValue
        }
        set {
            wrappedValue = .init(from: newValue)!
        }
    }
    
    /// Default initializer.
    public init(wrappedValue: Value) {
        self.wrappedValue = wrappedValue
    }
}

