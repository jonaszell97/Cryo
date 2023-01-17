
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
@propertyWrapper public struct CryoColumn<Value: _AnyCryoColumnValue> {
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

/// Property wrapper for assets in a CryoModel.
@propertyWrapper public struct CryoAsset {
    /// The wrapped, persistable value.
    public var wrappedValue: URL
    
    /// Default initializer.
    public init(wrappedValue: URL) {
        self.wrappedValue = wrappedValue
    }
}

extension CryoAsset: Codable {
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(wrappedValue)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.init(wrappedValue: try container.decode(URL.self))
    }
}

// MARK: Model reflection

internal typealias CryoSchema = [String: (CryoColumnType, (any CryoModel) -> _AnyCryoColumnValue)]

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
            
            
            let columnType: CryoColumnType
            let childTypeName = "\(childMirror.subjectType)"
            
            let wrappedValueMirror = Mirror(reflecting: wrappedValue.value)
            if childTypeName.starts(with: "CryoColumn") {
                switch wrappedValueMirror.subjectType {
                case is CryoColumnIntValue.Type: columnType = .integer
                case is CryoColumnDoubleValue.Type: columnType = .double
                case is CryoColumnStringValue.Type: columnType = .text
                case is CryoColumnDataValue.Type: columnType = .data
                default:
                    fatalError("\(wrappedValueMirror.subjectType) is not a valid type for a CryoColumn")
                }
            }
            else if childTypeName.starts(with: "CryoAsset") {
                columnType = .asset
            }
            else {
                continue
            }
            
            let extractValue: (any CryoModel) -> _AnyCryoColumnValue = { this in
                let mirror = Mirror(reflecting: this)
                let child = mirror.children.first { $0.label == label }!
                let childMirror = Mirror(reflecting: child.value)
                let wrappedValue = childMirror.children.first { $0.label == "wrappedValue" }!.value
                
                return wrappedValue as! _AnyCryoColumnValue
            }
            
            schema[name] = (columnType, extractValue)
        }
        
        return schema
    }
}
