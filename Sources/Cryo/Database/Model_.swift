
import Foundation

public final class CryoContext {
    /// Create a context.
    public init() {
        
    }
}

public protocol CryoClassModel: AnyObject, Codable, ObservableObject, _AnyCryoColumnValue {
    /// The name of the table representing this model.
    ///
    /// By default, the type name is used as the table name.
    static var tableName: String { get }
    
    /// The ID of this model.
    var id: String { get }
}

// MARK: Default values

public extension CryoContext {
    /// Create a default `Int` value.
    func defaultValue(for: Int.Type) -> Int { 0 }
    
    /// Create a default `UInt` value.
    func defaultValue(for: UInt.Type) -> UInt { 0 }
    
    /// Create a default `Boolean` value.
    func defaultValue(for: Bool.Type) -> Bool { false }
    
    /// Create a default `Double` value.
    func defaultValue(for: Double.Type) -> Double { 0 }
    
    /// Create a default `String` value.
    func defaultValue(for: String.Type) -> String { "" }
    
    /// Create a default `Date` value.
    func defaultValue(for: Date.Type) -> Date { Date() }
    
    /// Create a default `Data` value.
    func defaultValue(for: Data.Type) -> Data { Data() }
    
    /// Create a default `URL` value.
    func defaultValue(for: URL.Type) -> URL { URL(fileURLWithPath: "") }
    
    /// Create a default `Optional` value.
    func defaultValue<T>(for: Optional<T>.Type) -> Optional<T> { nil }
    
    /// Create a default `Codable` value.
    func defaultValue<T>(for: T.Type) -> T where T: Codable { try! T(from: EmptyDecoder()) }
}
