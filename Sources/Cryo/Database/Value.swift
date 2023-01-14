
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

public protocol CryoDatabaseValue: Codable {
    /// Empty initializer.
    init ()
}

// MARK: CryoDatabaseValue conformances

extension Int: CryoDatabaseValue {}
extension Int8: CryoDatabaseValue {}
extension Int16: CryoDatabaseValue {}
extension Int32: CryoDatabaseValue {}
extension Int64: CryoDatabaseValue {}
extension UInt: CryoDatabaseValue {}
extension UInt8: CryoDatabaseValue {}
extension UInt16: CryoDatabaseValue {}
extension UInt32: CryoDatabaseValue {}
extension UInt64: CryoDatabaseValue {}

extension Double: CryoDatabaseValue {}
extension Float: CryoDatabaseValue {}

extension String: CryoDatabaseValue {}
extension Bool: CryoDatabaseValue {}
extension Date: CryoDatabaseValue {}
extension Data: CryoDatabaseValue {}

extension URL: CryoDatabaseValue {
    public init() {
        self = URL(string: "file:///")!
    }
}
