
import Foundation

/// Decoder that constructs an empty value for any Decodable type.
internal class EmptyDecoder: Decoder {
    var codingPath: Array<CodingKey> { [] }
    var userInfo: Dictionary<CodingUserInfoKey, Any> { [:] }
    
    /// Default initializer.
    init() { }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        KeyedDecodingContainer(EmptyKeyedDecodingContainer<Key>())
    }
    
    func unkeyedContainer() throws -> UnkeyedDecodingContainer { EmptyUnkeyedDecodingContainer() }
    func singleValueContainer() throws -> SingleValueDecodingContainer { EmptySingleValueDecodingContainer() }
}

fileprivate class EmptyKeyedDecodingContainer<K: CodingKey>: KeyedDecodingContainerProtocol {
    typealias Key = K
    
    let codingPath: Array<CodingKey> = []
    var allKeys: Array<K> { [] }
    
    /// Default initializer.
    init() {}
    
    func contains(_ key: K) -> Bool { true }
    
    func decodeNil(forKey key: K) throws -> Bool { true }
    
    func decode(_ type: Bool.Type, forKey key: K) throws -> Bool { false }
    func decode(_ type: String.Type, forKey key: K) throws -> String { "" }
    func decode(_ type: Date.Type, forKey key: K) throws -> Date { .distantPast }
    func decode(_ type: Data.Type, forKey key: K) throws -> Data { .init() }
    
    func decode(_ type: Double.Type, forKey key: K) throws -> Double { 0.0 }
    func decode(_ type: Float.Type, forKey key: K) throws -> Float { 0.0 }
    
    func decode(_ type: Int.Type, forKey key: K) throws -> Int { 0 }
    func decode(_ type: Int8.Type, forKey key: K) throws -> Int8 { 0 }
    func decode(_ type: Int16.Type, forKey key: K) throws -> Int16 { 0 }
    func decode(_ type: Int32.Type, forKey key: K) throws -> Int32 { 0 }
    func decode(_ type: Int64.Type, forKey key: K) throws -> Int64 { 0 }
    func decode(_ type: UInt.Type, forKey key: K) throws -> UInt { 0 }
    func decode(_ type: UInt8.Type, forKey key: K) throws -> UInt8 { 0 }
    func decode(_ type: UInt16.Type, forKey key: K) throws -> UInt16 { 0 }
    func decode(_ type: UInt32.Type, forKey key: K) throws -> UInt32 { 0 }
    func decode(_ type: UInt64.Type, forKey key: K) throws -> UInt64 { 0 }
    
    func decode<T>(_ type: T.Type, forKey key: K) throws -> T where T: Decodable {
        if T.self == URL.self { return URL(string: "file:///") as! T }
        if T.self == Data.self { return Data() as! T }
        if let intType = T.self as? CryoColumnIntValue.Type { return intType.init(integerValue: 0) as! T }
        if let doubleType = T.self as? CryoColumnDoubleValue.Type { return doubleType.init(doubleValue: 0) as! T }
        if let stringType = T.self as? CryoColumnStringValue.Type { return stringType.init(stringValue: "") as! T }
        if let dataType = T.self as? CryoColumnDataValue.Type { return dataType.init(dataValue: .init()) as! T }
        
        return try T(from: EmptyDecoder())
    }
    
    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: K) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        KeyedDecodingContainer(EmptyKeyedDecodingContainer<NestedKey>())
    }
    
    func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer {
        EmptyUnkeyedDecodingContainer()
    }
    
    func superDecoder() throws -> Decoder { EmptyDecoder() }
    func superDecoder(forKey key: K) throws -> Decoder { EmptyDecoder() }
}

fileprivate struct EmptySingleValueDecodingContainer: SingleValueDecodingContainer {
    var codingPath: [CodingKey] { [] }
    
    /// Default initializer.
    init() {}
    
    func decodeNil() -> Bool { true }
    
    func decode(_ type: Bool.Type) throws -> Bool { false }
    func decode(_ type: String.Type) throws -> String { "" }
    func decode(_ type: Date.Type) throws -> Date { .distantPast }
    func decode(_ type: Data.Type) throws -> Data { .init() }
    
    func decode(_ type: Double.Type) throws -> Double { 0.0 }
    func decode(_ type: Float.Type) throws -> Float { 0.0 }
    
    func decode(_ type: Int.Type) throws -> Int { 0 }
    func decode(_ type: Int8.Type) throws -> Int8 { 0 }
    func decode(_ type: Int16.Type) throws -> Int16 { 0 }
    func decode(_ type: Int32.Type) throws -> Int32 { 0 }
    func decode(_ type: Int64.Type) throws -> Int64 { 0 }
    func decode(_ type: UInt.Type) throws -> UInt { 0 }
    func decode(_ type: UInt8.Type) throws -> UInt8 { 0 }
    func decode(_ type: UInt16.Type) throws -> UInt16 { 0 }
    func decode(_ type: UInt32.Type) throws -> UInt32 { 0 }
    func decode(_ type: UInt64.Type) throws -> UInt64 { 0 }

    func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if T.self == URL.self { return URL(string: "file:///") as! T }
        if T.self == Data.self { return Data() as! T }
        if let intType = T.self as? CryoColumnIntValue.Type { return intType.init(integerValue: 0) as! T }
        if let doubleType = T.self as? CryoColumnDoubleValue.Type { return doubleType.init(doubleValue: 0) as! T }
        if let stringType = T.self as? CryoColumnStringValue.Type { return stringType.init(stringValue: "") as! T }
        if let dataType = T.self as? CryoColumnDataValue.Type { return dataType.init(dataValue: .init()) as! T }
        
        return try T(from: EmptyDecoder())
    }
}

fileprivate struct EmptyUnkeyedDecodingContainer: UnkeyedDecodingContainer {
    var count: Int?
    var codingPath: [CodingKey] { [] }
    var isAtEnd: Bool { true }
    var currentIndex: Int { 0 }
    
    /// Default initializer.
    init(count: Int? = nil) { self.count = count }
    
    mutating func decodeNil() throws -> Bool { true }
    mutating func decode(_ type: Bool.Type) throws -> Bool     { false }
    mutating func decode(_ type: String.Type) throws -> String { "" }
    mutating func decode(_ type: Date.Type) throws -> Date { .distantPast }
    mutating func decode(_ type: Data.Type) throws -> Data { .init() }
    
    mutating func decode(_ type: Double.Type) throws -> Double { 0 }
    mutating func decode(_ type: Float.Type) throws -> Float   { 0 }
    
    mutating func decode(_ type: Int.Type) throws -> Int       { 0 }
    mutating func decode(_ type: Int8.Type) throws -> Int8     { 0 }
    mutating func decode(_ type: Int16.Type) throws -> Int16   { 0 }
    mutating func decode(_ type: Int32.Type) throws -> Int32   { 0 }
    mutating func decode(_ type: Int64.Type) throws -> Int64   { 0 }
    mutating func decode(_ type: UInt.Type) throws -> UInt     { 0 }
    mutating func decode(_ type: UInt8.Type) throws -> UInt8   { 0 }
    mutating func decode(_ type: UInt16.Type) throws -> UInt16 { 0 }
    mutating func decode(_ type: UInt32.Type) throws -> UInt32 { 0 }
    mutating func decode(_ type: UInt64.Type) throws -> UInt64 { 0 }

    mutating func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if T.self == URL.self { return URL(string: "file:///") as! T }
        if T.self == Data.self { return Data() as! T }
        if let intType = T.self as? CryoColumnIntValue.Type { return intType.init(integerValue: 0) as! T }
        if let doubleType = T.self as? CryoColumnDoubleValue.Type { return doubleType.init(doubleValue: 0) as! T }
        if let stringType = T.self as? CryoColumnStringValue.Type { return stringType.init(stringValue: "") as! T }
        if let dataType = T.self as? CryoColumnDataValue.Type { return dataType.init(dataValue: .init()) as! T }
        
        return try T(from: EmptyDecoder())
    }
    
    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer { EmptyUnkeyedDecodingContainer() }
    mutating func superDecoder() throws -> Decoder { EmptyDecoder() }
    mutating func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        KeyedDecodingContainer(EmptyKeyedDecodingContainer<NestedKey>())
    }
}
