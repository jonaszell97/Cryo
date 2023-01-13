
import Foundation

/// Protocol for types that can be persisted using CloudKit.
public protocol CryoModel: Codable {
    /// The name for the table representing this model.
    static var tableName: String { get }
}

extension CryoModel {
    /// The name for the table representing this model.
    public static var tableName: String { "\(Self.self)" }
}

/// Property wrapper for columns in a CryoModel.
@propertyWrapper public struct CryoColumn<Value: CryoDatabaseValue> {
    /// The wrapped, persistable value.
    public var wrappedValue: Value
    
    /// Default initializer.
    public init(wrappedValue: Value) {
        self.wrappedValue = wrappedValue
    }
}

extension CryoColumn: Codable {
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(wrappedValue)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.init(wrappedValue: try container.decode(Value.self))
    }
}

// MARK: Model reflection

internal typealias CryoSchema = [String: (CryoColumnType, (any CryoModel) -> any CryoDatabaseValue)]

internal extension CryoModel {
    static var schema: CryoSchema {
        var schema = CryoSchema()
        
        // Create an empty instance and find columns from it
        let emptyInstance = try! Self(from: EmptyDecoder())
        let mirror = Mirror(reflecting: emptyInstance)
        
        for child in mirror.children {
            guard
                let label = child.label,
                label.hasPrefix("_")
            else {
                continue
            }
            
            let name = "\(label.dropFirst())"
            guard !name.isEmpty else {
                continue
            }
            
            let childMirror = Mirror(reflecting: child.value)
            let wrappedValue = childMirror.children.first {
                $0.label == "wrappedValue"
            }
            
            guard let wrappedValue else {
                continue
            }
            
            let wrappedValueMirror = Mirror(reflecting: wrappedValue.value)
            
            let columnType: CryoColumnType
            switch wrappedValueMirror.subjectType {
            case is String.Type: columnType = .text
            case is URL.Type:    columnType = .text
                
            case is Double.Type: columnType = .double
            case is Float.Type:  columnType = .double
                
            case is Bool.Type:   columnType = .bool
                
            case is Int.Type:    columnType = .integer
            case is Int.Type:    columnType = .integer
            case is Int8.Type:   columnType = .integer
            case is Int16.Type:  columnType = .integer
            case is Int32.Type:  columnType = .integer
            case is Int64.Type:  columnType = .integer
            case is UInt.Type:   columnType = .integer
            case is UInt8.Type:  columnType = .integer
            case is UInt16.Type: columnType = .integer
            case is UInt32.Type: columnType = .integer
                
            case is Date.Type:   columnType = .date
            case is Data.Type:   columnType = .data
            default:
                fatalError("\(wrappedValueMirror.subjectType) is not a valid type for a CryoColumn")
            }
            
            let extractValue: (any CryoModel) -> any CryoDatabaseValue = { this in
                let mirror = Mirror(reflecting: this)
                let child = mirror.children.first { $0.label == label }!
                let childMirror = Mirror(reflecting: child.value)
                let wrappedValue = childMirror.children.first { $0.label == "wrappedValue" }!.value
                
                return wrappedValue as! any CryoDatabaseValue
            }
            
            schema[name] = (columnType, extractValue)
        }
        
        return schema
    }
}
