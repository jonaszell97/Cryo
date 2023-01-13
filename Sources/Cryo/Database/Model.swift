
import Foundation

public typealias CryoModelDetails<Value> = [String: (CryoValue.ValueType, WritableKeyPath<Value, CryoValue>)]

/// Protocol for types that can be persisted using CloudKit.
public protocol CryoModel: Codable {
    /// The name for the table representing this model.
    static var tableName: String { get }
}

extension CryoModel {
    /// The name for the table representing this model.
    public static var tableName: String { "\(Self.self)" }
}

internal protocol AnyCryoColumn {
    init()
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

extension CryoColumn: AnyCryoColumn {
    /// Empty initializer.
    init() {
        self.wrappedValue = .init()
    }
}

extension CryoColumn: Codable {
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(try wrappedValue.persistableValue)
    }
    
    public init(from decoder: Decoder) throws {
        if decoder is EmptyDecoder {
            self.init()
            return
        }
        
        let container = try decoder.singleValueContainer()
        let cryoValue = try container.decode(CryoValue.self)
        
        guard let value = Value(from: cryoValue) else {
            throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath, debugDescription: "failed to decode CryoValue"))
        }
        
        self.init(wrappedValue: value)
    }
}

// MARK: Model reflection

internal typealias CryoSchema = [String: (CryoValue.ValueType, (any CryoModel) -> CryoValue)]

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
            
            let columnType: CryoValue.ValueType
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
            
            let extractValue: (any CryoModel) -> CryoValue = { this in
                let mirror = Mirror(reflecting: this)
                let child = mirror.children.first { $0.label == label }!
                let childMirror = Mirror(reflecting: child.value)
                let wrappedValue = childMirror.children.first { $0.label == "wrappedValue" }!.value
                
                switch wrappedValue {
                case let v as String:
                    return .text(value: v)
                case let v as URL:
                    return .text(value: v.absoluteString)
                    
                case let v as Double:
                    return .double(value: v)
                case let v as Float:
                    return .double(value: Double(v))
                
                case let v as Bool:
                    return .bool(value: v)
                    
                case let v as Int:
                    return .integer(value: v)
                case let v as Int8:
                    return .integer(value: Int(v))
                case let v as Int16:
                    return .integer(value: Int(v))
                case let v as Int32:
                    return .integer(value: Int(v))
                case let v as Int64:
                    return .integer(value: Int(v))
                case let v as UInt:
                    return .integer(value: Int(v))
                case let v as UInt8:
                    return .integer(value: Int(v))
                case let v as UInt16:
                    return .integer(value: Int(v))
                case let v as UInt32:
                    return .integer(value: Int(v))
                case let v as UInt64:
                    return .integer(value: Int(v))
                    
                case let v as Date:
                    return .date(value: v)
                case let v as Data:
                    return .data(value: v)
                    
                default:
                    fatalError("\(wrappedValueMirror.subjectType) is not a valid type for a CryoColumn")
                }
                
            }
            
            schema[name] = (columnType, extractValue)
        }
        
        return schema
    }
}
