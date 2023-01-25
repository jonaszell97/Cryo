
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

public protocol _AnyCryoColumnValue: Codable {
    
}

public protocol CryoColumnIntValue: _AnyCryoColumnValue {
    /// The integer value of this instance.
    var integerValue: Int64 { get }
    
    /// Initialize from an integer value.
    init (integerValue: Int64)
}

public protocol CryoColumnDoubleValue: _AnyCryoColumnValue {
    /// The double value of this instance.
    var doubleValue: Double { get }
    
    /// Initialize from an integer value.
    init (doubleValue: Double)
}

public protocol CryoColumnStringValue: _AnyCryoColumnValue {
    /// The string value of this instance.
    var stringValue: String { get }
    
    /// Initialize from a string value.
    init (stringValue: String)
}

public protocol CryoColumnDataValue: _AnyCryoColumnValue {
    /// The data value of this instance.
    var dataValue: Data { get throws }
    
    /// Initialize from a data value.
    init (dataValue: Data) throws
}

// MARK: CryoDatabaseValue conformances

extension Int: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
}

extension Int8: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
}

extension Int16: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
}

extension Int32: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
}

extension Int64: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { self }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = integerValue }
}

extension UInt: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
}

extension UInt8: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
}

extension UInt16: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
}

extension UInt32: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { Int64(self) }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(integerValue) }
}

extension Bool: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { self ? 1 : 0 }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = integerValue != 0 }
}

extension RawRepresentable where RawValue: CryoColumnIntValue {
    /// The integer value of this instance.
    public var integerValue: Int64 { self.rawValue.integerValue }
    
    /// Initialize from an integer value.
    public init (integerValue: Int64) { self = Self(rawValue: .init(integerValue: integerValue))! }
}

extension Double: CryoColumnDoubleValue {
    /// The double value of this instance.
    public var doubleValue: Double { self }
    
    /// Initialize from an integer value.
    public init (doubleValue: Double) { self = doubleValue }
}

extension Float: CryoColumnDoubleValue {
    /// The double value of this instance.
    public var doubleValue: Double { Double(self) }
    
    /// Initialize from an integer value.
    public init (doubleValue: Double) { self = Self(doubleValue) }
}

extension Date: CryoColumnDoubleValue {
    /// The double value of this instance.
    public var doubleValue: Double { Double(self.timeIntervalSinceReferenceDate) }
    
    /// Initialize from an integer value.
    public init (doubleValue: Double) { self = Date(timeIntervalSinceReferenceDate: doubleValue) }
}

extension RawRepresentable where RawValue: CryoColumnDoubleValue {
    /// The double value of this instance.
    public var doubleValue: Double { self.rawValue.doubleValue }
    
    /// Initialize from an integer value.
    public init (doubleValue: Double) { self = Self(rawValue: .init(doubleValue: doubleValue))! }
}

extension String: CryoColumnStringValue {
    /// The string value of this instance.
    public var stringValue: String { self }
    
    /// Initialize from a string value.
    public init (stringValue: String) { self = stringValue }
}

extension URL: CryoColumnStringValue {
    /// The string value of this instance.
    public var stringValue: String { self.absoluteString }
    
    /// Initialize from a string value.
    public init (stringValue: String) { self.init(string: stringValue)! }
}

extension RawRepresentable where RawValue: CryoColumnStringValue {
    /// The string value of this instance.
    public var stringValue: String { self.rawValue.stringValue }
    
    /// Initialize from a string value.
    public init (stringValue: String) { self = Self(rawValue: .init(stringValue: stringValue))! }
}

extension Data: CryoColumnDataValue {
    /// The data value of this instance.
    public var dataValue: Data { self }
    
    /// Initialize from a data value.
    public init (dataValue: Data) { self = dataValue }
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
