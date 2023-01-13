
import Foundation

public enum CryoValue {
    /// Data type for integers. Stored as an NSNumber.
    case integer(value: Int)
    
    /// Data type for numbers. Stored as an NSNumber.
    case double(value: Double)
    
    /// Data type for strings. Stored as an NSString.
    case text(value: String)
    
    /// Data type for dates. Stored as an NSDate.
    case date(value: Date)
    
    /// Data type for booleans. Stored as an NSNumber.
    case bool(value: Bool)
    
    /// Data type for raw data. Stored as NSData.
    case data(value: Data)
}

public protocol CryoPersistable: Codable {
    /// The value type of this persistable value.
    static var valueType: CryoValue.ValueType { get }
    
    /// The persistable value.
    var persistableValue: CryoValue { get throws }
    
    /// Initialize from a persisted value.
    init? (from value: CryoValue)
    
    /// Empty initializer.
    init ()
}

// MARK: Default conformances

extension CryoPersistable {
    /// Convenience initializer for codable data.
    public static func data<Value: Codable>(_ value: Value) throws -> CryoValue {
        .data(value: try JSONEncoder().encode(value))
    }
}

extension CryoPersistable where Self: Codable {
    /// The value type of this persistable value.
    public static var valueType: CryoValue.ValueType { .data }
    
    /// The persistable value.
    public var persistableValue: CryoValue {
        get throws {
            .data(value: try JSONEncoder().encode(self))
        }
    }
    
    /// Initialize from a persisted value.
    public init? (from value: CryoValue) {
        guard case .data(let data) = value else {
            return nil
        }

        guard let this = try? JSONDecoder().decode(Self.self, from: data) else {
            return nil
        }
        
        self = this
    }
}

// MARK: CryoPersistable conformances

extension Int: CryoPersistable {
    public static var valueType: CryoValue.ValueType { .integer }
    
    public var persistableValue: CryoValue {
        .integer(value: self)
    }
    
    public init? (from value: CryoValue) {
        guard case .integer(let value) = value else {
            return nil
        }
        
        self = value
    }
}

extension Double: CryoPersistable {
    public static var valueType: CryoValue.ValueType { .double }
    
    public var persistableValue: CryoValue {
        .double(value: self)
    }
    
    public init? (from value: CryoValue) {
        guard case .double(let value) = value else {
            return nil
        }
        
        self = value
    }
}

extension String: CryoPersistable {
    public static var valueType: CryoValue.ValueType { .text }
    
    public var persistableValue: CryoValue {
        .text(value: self)
    }
    
    public init? (from value: CryoValue) {
        guard case .text(let value) = value else {
            return nil
        }
        
        self = value
    }
}

extension Date: CryoPersistable {
    public static var valueType: CryoValue.ValueType { .date }
    
    public var persistableValue: CryoValue {
        .date(value: self)
    }
    
    public init? (from value: CryoValue) {
        guard case .date(let value) = value else {
            return nil
        }
        
        self = value
    }
}

extension Bool: CryoPersistable {
    public static var valueType: CryoValue.ValueType { .bool }
    
    public var persistableValue: CryoValue {
        .bool(value: self)
    }
    
    public init? (from value: CryoValue) {
        guard case .bool(let value) = value else {
            return nil
        }
        
        self = value
    }
}

extension Data: CryoPersistable {
    public static var valueType: CryoValue.ValueType { .data }
    
    public var persistableValue: CryoValue {
        .data(value: self)
    }
    
    public init? (from value: CryoValue) {
        guard case .data(let value) = value else {
            return nil
        }
        
        self = value
    }
}

// MARK: Conformances

extension CryoValue: Codable {
    public enum ValueType: String, CodingKey {
        case integer, double, text, date, bool, data
    }
    
    var codingKey: ValueType {
        switch self {
        case .integer: return .integer
        case .double: return .double
        case .text: return .text
        case .date: return .date
        case .bool: return .bool
        case .data: return .data
        }
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: ValueType.self)
        switch self {
        case .integer(let value):
            try container.encode(value, forKey: .integer)
        case .double(let value):
            try container.encode(value, forKey: .double)
        case .text(let value):
            try container.encode(value, forKey: .text)
        case .date(let value):
            try container.encode(value, forKey: .date)
        case .bool(let value):
            try container.encode(value, forKey: .bool)
        case .data(let value):
            try container.encode(value, forKey: .data)
        }
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: ValueType.self)
        switch container.allKeys.first {
        case .integer:
            let value = try container.decode(Int.self, forKey: .integer)
            self = .integer(value: value)
        case .double:
            let value = try container.decode(Double.self, forKey: .double)
            self = .double(value: value)
        case .text:
            let value = try container.decode(String.self, forKey: .text)
            self = .text(value: value)
        case .date:
            let value = try container.decode(Date.self, forKey: .date)
            self = .date(value: value)
        case .bool:
            let value = try container.decode(Bool.self, forKey: .bool)
            self = .bool(value: value)
        case .data:
            let value = try container.decode(Data.self, forKey: .data)
            self = .data(value: value)
        default:
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: container.codingPath,
                    debugDescription: "Unabled to decode enum."
                )
            )
        }
    }
}

extension CryoValue: Equatable {
    public static func ==(lhs: CryoValue, rhs: CryoValue) -> Bool {
        guard lhs.codingKey == rhs.codingKey else {
            return false
        }
        
        switch lhs {
        case .integer(let value):
            guard case .integer(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .double(let value):
            guard case .double(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .text(let value):
            guard case .text(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .date(let value):
            guard case .date(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .bool(let value):
            guard case .bool(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .data(let value):
            guard case .data(let value_) = rhs else { return false }
            guard value == value_ else { return false }
            
        }
        
        return true
    }
}

extension CryoValue: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.codingKey.rawValue)
        switch self {
        case .integer(let value):
            hasher.combine(value)
        case .double(let value):
            hasher.combine(value)
        case .text(let value):
            hasher.combine(value)
        case .date(let value):
            hasher.combine(value)
        case .bool(let value):
            hasher.combine(value)
        case .data(let value):
            hasher.combine(value)
            
        }
    }
}


