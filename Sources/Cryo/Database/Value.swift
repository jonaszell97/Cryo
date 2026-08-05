
import Foundation

internal enum CryoColumnType {
    /// Data type for integers. Stored as an NSNumber.
    case integer
    
    /// Data type for numbers. Stored as an NSNumber.
    case double
    
    /// Data type for strings. Stored as an NSString.
    case text
    
    /// Data type for dates. Stored as an NSDate.
    case date
    
    /// Data type for booleans. Stored as an NSNumber.
    case bool
    
    /// Data type for raw data. Stored as NSData.
    case data
    
    /// A CloudKit asset.
    case asset
}

/// Protocol for types that can be stored in a CloudKIt column.
public protocol _AnyCryoColumnValue: Codable {
    /// A default value for this type.
    static var defaultValue: Self { get }
}

/// Protocol for types that can be stored in a CloudKIt column as an `Int64`.
public protocol CryoColumnIntValue: _AnyCryoColumnValue {
    /// The integer value of this instance.
    var integerValue: Int64 { get }
    
    /// Initialize from an integer value.
    init (integerValue: Int64)
}

/// Protocol for types that can be stored in a CloudKIt column as a `Double`.
public protocol CryoColumnDoubleValue: _AnyCryoColumnValue {
    /// The double value of this instance.
    var doubleValue: Double { get }
    
    /// Initialize from an integer value.
    init (doubleValue: Double)
}

/// Protocol for types that can be stored in a CloudKIt column as a `String`.
public protocol CryoColumnStringValue: _AnyCryoColumnValue {
    /// The string value of this instance.
    var stringValue: String { get }
    
    /// Initialize from a string value.
    init (stringValue: String)
}

/// Protocol for types that can be stored in a CloudKIt column as a `Date`.
public protocol CryoColumnDateValue: _AnyCryoColumnValue {
    /// The date value of this instance.
    var dateValue: Date { get }
    
    /// Initialize from a date value.
    init (dateValue: Date)
}

/// Protocol for types that can be stored in a CloudKIt column as `Data`.
public protocol CryoColumnDataValue: _AnyCryoColumnValue {
    /// The data value of this instance.
    var dataValue: Data { get throws }
    
    /// Initialize from a data value.
    init (dataValue: Data) throws
}

public extension _AnyCryoColumnValue {
    /// A default value for this type.
    static var defaultValue: Self { try! .init(from: EmptyDecoder()) }
}

internal protocol _CryoOptionalValue {
    /// The column type of this optional.
    static var columnType: CryoColumnType { get }
    
    /// The wrapped value, if present.
    var wrappedValue: _AnyCryoColumnValue? { get }
    
    /// The nil value.
    static var nilValue: Self { get }
}

extension Optional: _AnyCryoColumnValue, _CryoOptionalValue where Wrapped: _AnyCryoColumnValue {
    /// The column type of this optional.
    static var columnType: CryoColumnType {
        switch Wrapped.self {
        case is CryoColumnIntValue.Type: return .integer
        case is CryoColumnDoubleValue.Type: return .double
        case is CryoColumnStringValue.Type: return .text
        case is CryoColumnDateValue.Type: return .date
        case is CryoColumnDataValue.Type: return .data
        default:
            fatalError("\(Wrapped.self) is not a valid type for a CryoColumn")
        }
    }
    
    /// The wrapped value, if present.
    var wrappedValue: _AnyCryoColumnValue? {
        guard case .some(let wrapped) = self else {
            return nil
        }
        
        return wrapped
    }
    
    /// A default value for this type.
    public static var defaultValue: Self { .some(Wrapped.defaultValue) }
    
    /// The nil value.
    static var nilValue: Self { nil }
}

// MARK: CryoDatabaseValue conformances

extension Int: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension Int8: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension Int16: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension Int32: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension Int64: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { self }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = integerValue }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension UInt: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension UInt8: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension UInt16: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension UInt32: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension Bool: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { self ? 1 : 0 }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = integerValue != 0 }
    
    /// A default value for this type.
    public static var defaultValue: Self { false }
}

extension RawRepresentable where RawValue: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { self.rawValue.integerValue }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(rawValue: .init(integerValue: integerValue))! }
    
    /// A default value for this type.
    public static var defaultValue: Self { .init(rawValue: RawValue.defaultValue)! }
}

extension Optional: CryoColumnIntValue where Wrapped: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 {
        guard case .some(let wrapped) = self else {
            return 0
        }
        
        return wrapped.integerValue
    }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = .some(.init(integerValue: integerValue)) }
}

extension Double: CryoColumnDoubleValue {
    /// The double value of this instance.
    public var doubleValue: Double { self }
    
    /// Initialize from an integer value.
    public init (doubleValue: Double) { self = doubleValue }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension Float: CryoColumnDoubleValue {
    /// The double value of this instance.
    public var doubleValue: Double { Double(self) }
    
    /// Initialize from an integer value.
    public init (doubleValue: Double) { self = Self(doubleValue) }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension Date: CryoColumnDateValue {
    /// The date value of this instance.
    public var dateValue: Date { self }
    
    /// Initialize from a date value.
    public init (dateValue: Date) { self = dateValue }
    
    /// A default value for this type.
    public static var defaultValue: Self { Date() }
}

extension RawRepresentable where RawValue: CryoColumnDoubleValue {
    /// The double value of this instance.
    public var doubleValue: Double { self.rawValue.doubleValue }
    
    /// Initialize from an integer value.
    public init (doubleValue: Double) { self = Self(rawValue: .init(doubleValue: doubleValue))! }
}

extension Optional: CryoColumnDoubleValue where Wrapped: CryoColumnDoubleValue {
    /// The double value of this instance.
    public var doubleValue: Double {
        guard case .some(let wrapped) = self else {
            return 0
        }
        
        return wrapped.doubleValue
    }
    
    /// Initialize from an integer value.
    public init (doubleValue: Double) { self = .some(.init(doubleValue: doubleValue)) }
}

extension String: CryoColumnStringValue {
    /// The string value of this instance.
    public var stringValue: String { self }
    
    /// Initialize from a string value.
    public init (stringValue: String) { self = stringValue }
    
    /// A default value for this type.
    public static var defaultValue: Self { "" }
}

extension URL: CryoColumnStringValue {
    /// The string value of this instance.
    public var stringValue: String { self.absoluteString }
    
    /// Initialize from a string value.
    public init (stringValue: String) { self.init(string: stringValue)! }
    
    /// A default value for this type.
    public static var defaultValue: Self { URL(string: "file:///")! }
}

extension UUID: CryoColumnStringValue {
    /// The string value of this instance.
    public var stringValue: String { self.uuidString }
    
    /// Initialize from a string value.
    public init (stringValue: String) { self = .init(uuidString: stringValue) ?? Self.defaultValue }
    
    /// A default value for this type.
    public static var defaultValue: Self { UUID(uuidString: "00000000-0000-0000-0000-000000000000")! }
}

extension Decimal: CryoColumnStringValue {
    /// The locale used for encoding and decoding decimals.
    fileprivate static let codingLocale = Locale(identifier: "en_US")
    
    /// The string value of this instance.
    public var stringValue: String {
        var copy = self
        return NSDecimalString(&copy, Self.codingLocale)
    }
    
    /// Initialize from a string value.
    public init (stringValue: String) { self = Decimal(string: stringValue, locale: Self.codingLocale) ?? 0 }
    
    /// A default value for this type.
    public static var defaultValue: Self { 0 }
}

extension Optional: CryoColumnStringValue where Wrapped: CryoColumnStringValue {
    /// The string value of this instance.
    public var stringValue: String {
        guard case .some(let wrapped) = self else {
            return ""
        }
        
        return wrapped.stringValue
    }
    
    /// Initialize from a string value.
    public init (stringValue: String) { self = .some(.init(stringValue: stringValue)) }
}

extension RawRepresentable where RawValue: CryoColumnStringValue, Self: CaseIterable {
    /// The string value of this instance.
    public var stringValue: String { self.rawValue.stringValue }
    
    /// Initialize from a string value.
    public init (stringValue: String) { self = Self(rawValue: .init(stringValue: stringValue)) ?? .allCases.first! }
}

extension Data: CryoColumnDataValue {
    /// The data value of this instance.
    public var dataValue: Data { self }
    
    /// Initialize from a data value.
    public init (dataValue: Data) { self = dataValue }
    
    /// A default value for this type.
    public static var defaultValue: Self { Data() }
}

extension Optional: CryoColumnDataValue where Wrapped: CryoColumnDataValue {
    /// The data value of this instance.
    public var dataValue: Data {
        get throws {
            guard case .some(let wrapped) = self else {
                return Data()
            }
            
            return try wrapped.dataValue
        }
    }
    
    /// Initialize from a data value.
    public init (dataValue: Data) throws { self = .some(try .init(dataValue: dataValue)) }
}

extension Encodable {
    /// The data value of this instance.
    public var dataValue: Data {
        get throws {
            try JSONEncoder().encode(self)
        }
    }
}

extension Decodable {
    /// Initialize from a data value.
    public init (dataValue: Data) throws {
        self = try JSONDecoder().decode(Self.self, from: dataValue)
    }
}

extension Array: _AnyCryoColumnValue, CryoColumnDataValue where Element: _AnyCryoColumnValue {
    /// Initialize from a data value.
    public init (dataValue: Data) throws {
        guard !dataValue.isEmpty else {
            self = []
            return
        }
        
        self = try JSONDecoder().decode(Self.self, from: dataValue)
    }
    
    /// A default value for this type.
    public static var defaultValue: Self { [] }
}

extension Dictionary: _AnyCryoColumnValue, CryoColumnDataValue where Key: _AnyCryoColumnValue, Value: _AnyCryoColumnValue {
    /// Initialize from a data value.
    public init (dataValue: Data) throws {
        guard !dataValue.isEmpty else {
            self = [:]
            return
        }
        
        self = try JSONDecoder().decode(Self.self, from: dataValue)
    }
    
    /// A default value for this type.
    public static var defaultValue: Self { [:] }
}
