
import Foundation

/// Decoder that constructs a CryoModel value from a set of key-value pairs fetched from a database.
internal class CryoModelDecoder: Decoder {
    var codingPath: Array<CodingKey> { [] }
    var userInfo: Dictionary<CodingUserInfoKey, Any> { [:] }
    
    var data: [String: _AnyCryoColumnValue]
    
    /// Default initializer.
    init(data: [String: _AnyCryoColumnValue]) {
        self.data = data
    }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        KeyedDecodingContainer(CryoModelKeyedDecodingContainer<Key>(data: data))
    }
    
    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        fatalError("nested containers are not supported in CryoModel")
    }
    
    func singleValueContainer() throws -> SingleValueDecodingContainer {
        fatalError("nested containers are not supported in CryoModel")
    }
}

fileprivate class CryoModelKeyedDecodingContainer<K: CodingKey>: KeyedDecodingContainerProtocol {
    typealias Key = K
    
    let codingPath: Array<CodingKey> = []
    var allKeys: Array<K> { [] }
    
    var data: [String: _AnyCryoColumnValue]
    
    /// Default initializer.
    init(data: [String: _AnyCryoColumnValue]) {
        self.data = data
    }
    
    func contains(_ key: K) -> Bool { data[key.stringValue] != nil }
    
    func decodeNil(forKey key: K) throws -> Bool { false }
    
    func decode(_ type: Bool.Type, forKey key: K) throws -> Bool {
        guard let data = self.data[key.stringValue], let value = data as? Bool else {
            throw DecodingError.keyNotFound(key, .init(codingPath: codingPath, debugDescription: ""))
        }
        
        return value
    }
    
    func decode(_ type: String.Type, forKey key: K) throws -> String {
        guard let data = self.data[key.stringValue], let value = data as? String else {
            throw DecodingError.keyNotFound(key, .init(codingPath: codingPath, debugDescription: ""))
        }
        
        return value
    }
    
    func decode(_ type: Date.Type, forKey key: K) throws -> Date {
        guard let data = self.data[key.stringValue], let value = data as? Date else {
            throw DecodingError.keyNotFound(key, .init(codingPath: codingPath, debugDescription: ""))
        }
        
        return value
    }
    
    func decode(_ type: Data.Type, forKey key: K) throws -> Data {
        guard let data = self.data[key.stringValue], let value = data as? Data else {
            throw DecodingError.keyNotFound(key, .init(codingPath: codingPath, debugDescription: ""))
        }
        
        return value
    }
    
    func decode(_ type: Double.Type, forKey key: K) throws -> Double {
        guard let data = self.data[key.stringValue], let value = data as? Double else {
            throw DecodingError.keyNotFound(key, .init(codingPath: codingPath, debugDescription: ""))
        }
        
        return value
    }
    
    func decode(_ type: Float.Type, forKey key: K) throws -> Float {
        guard let data = self.data[key.stringValue], let value = data as? Float else {
            throw DecodingError.keyNotFound(key, .init(codingPath: codingPath, debugDescription: ""))
        }
        
        return Float(value)
    }
    
    func decodeInt<T: BinaryInteger>(_ type: T.Type, forKey key: K) throws -> T {
        guard let data = self.data[key.stringValue], let value = data as? T else {
            throw DecodingError.keyNotFound(key, .init(codingPath: codingPath, debugDescription: ""))
        }
        
        return T(value)
    }
    
    func decode(_ type: Int.Type, forKey key: K) throws -> Int { try decodeInt(Int.self, forKey: key) }
    func decode(_ type: Int8.Type, forKey key: K) throws -> Int8 { try decodeInt(Int8.self, forKey: key) }
    func decode(_ type: Int16.Type, forKey key: K) throws -> Int16 { try decodeInt(Int16.self, forKey: key) }
    func decode(_ type: Int32.Type, forKey key: K) throws -> Int32 { try decodeInt(Int32.self, forKey: key) }
    func decode(_ type: Int64.Type, forKey key: K) throws -> Int64 { try decodeInt(Int64.self, forKey: key) }
    func decode(_ type: UInt.Type, forKey key: K) throws -> UInt { try decodeInt(UInt.self, forKey: key) }
    func decode(_ type: UInt8.Type, forKey key: K) throws -> UInt8 { try decodeInt(UInt8.self, forKey: key) }
    func decode(_ type: UInt16.Type, forKey key: K) throws -> UInt16 { try decodeInt(UInt16.self, forKey: key) }
    func decode(_ type: UInt32.Type, forKey key: K) throws -> UInt32 { try decodeInt(UInt32.self, forKey: key) }
    func decode(_ type: UInt64.Type, forKey key: K) throws -> UInt64 { fatalError("UInt64 cannot be represented") }
    
    func decode<T>(_ type: T.Type, forKey key: K) throws -> T where T: Decodable {
        guard let data = self.data[key.stringValue] else {
            throw DecodingError.keyNotFound(key, .init(codingPath: codingPath, debugDescription: ""))
        }
        
        return try T(from: CryoModelValueDecoder(value: data))
    }
    
    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: K) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        fatalError("nested containers are not supported in CryoModel")
    }
    
    func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer {
        fatalError("nested containers are not supported in CryoModel")
    }
    
    func superDecoder() throws -> Decoder {
        fatalError("super decoders are not supported in CryoModel")
    }
    
    func superDecoder(forKey key: K) throws -> Decoder {
        fatalError("super decoders are not supported in CryoModel")
    }
}

internal class CryoModelValueDecoder: Decoder {
    var codingPath: Array<CodingKey> { [] }
    var userInfo: Dictionary<CodingUserInfoKey, Any> { [:] }
    
    let value: _AnyCryoColumnValue
    
    /// Default initializer.
    init(value: _AnyCryoColumnValue) {
        self.value = value
    }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        fatalError("nested containers are not supported in CryoModel")
    }
    
    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        fatalError("nested containers are not supported in CryoModel")
    }
    
    func singleValueContainer() throws -> SingleValueDecodingContainer {
        CryoModelSingleValueDecodingContainer(value: value)
    }
}

fileprivate struct CryoModelSingleValueDecodingContainer: SingleValueDecodingContainer {
    var codingPath: [CodingKey] { [] }
    let value: _AnyCryoColumnValue

    /// Default initializer.
    init(value: _AnyCryoColumnValue) {
        self.value = value
    }

    func decodeNil() -> Bool { false }

    func decode(_ type: Bool.Type) throws -> Bool {
        guard let value = value as? Bool else {
            throw DecodingError.typeMismatch(Bool.self, .init(codingPath: codingPath, debugDescription: "unexpected CryoPersistable: \(value)"))
        }

        return value
    }

    func decode(_ type: String.Type) throws -> String {
        guard let value = value as? String else {
            throw DecodingError.typeMismatch(String.self, .init(codingPath: codingPath, debugDescription: "unexpected CryoPersistable: \(value)"))
        }

        return value
    }

    func decode(_ type: Date.Type) throws -> Date {
        guard let value = value as? Date else {
            throw DecodingError.typeMismatch(Date.self, .init(codingPath: codingPath, debugDescription: "unexpected CryoPersistable: \(value)"))
        }

        return value
    }

    func decode(_ type: Data.Type) throws -> Data {
        guard let value = value as? Data else {
            throw DecodingError.typeMismatch(Data.self, .init(codingPath: codingPath, debugDescription: "unexpected CryoPersistable: \(value)"))
        }

        return value
    }

    func decode(_ type: Double.Type) throws -> Double {
        guard let value = value as? Double else {
            throw DecodingError.typeMismatch(Double.self, .init(codingPath: codingPath, debugDescription: "unexpected CryoPersistable: \(value)"))
        }

        return value
    }

    func decode(_ type: Float.Type) throws -> Float {
        guard let value = value as? Float else {
            throw DecodingError.typeMismatch(Float.self, .init(codingPath: codingPath, debugDescription: "unexpected CryoPersistable: \(value)"))
        }

        return Float(value)
    }
    
    func decode(_ type: URL.Type) throws -> URL {
        guard let value = value as? URL else {
            throw DecodingError.typeMismatch(URL.self, .init(codingPath: codingPath, debugDescription: "unexpected CryoPersistable: \(value)"))
        }
        
        return value
    }

    func decodeInt<T: BinaryInteger>(_ type: T.Type) throws -> T {
        guard let value = value as? CryoColumnIntValue else {
            throw DecodingError.typeMismatch(T.self, .init(codingPath: codingPath, debugDescription: "unexpected CryoPersistable: \(value)"))
        }

        return T(value.integerValue)
    }
    
    func decode<T: _AnyCryoColumnValue>(_ type: [T].Type) throws -> [T] {
        guard let value = value as? CryoColumnDataValue else {
            throw DecodingError.typeMismatch(T.self, .init(codingPath: codingPath, debugDescription: "unexpected CryoPersistable: \(value)"))
        }

        return try [T](dataValue: value.dataValue)
    }

    func decode(_ type: Int.Type) throws -> Int { try decodeInt(Int.self) }
    func decode(_ type: Int8.Type) throws -> Int8 { try decodeInt(Int8.self) }
    func decode(_ type: Int16.Type) throws -> Int16 { try decodeInt(Int16.self) }
    func decode(_ type: Int32.Type) throws -> Int32 { try decodeInt(Int32.self) }
    func decode(_ type: Int64.Type) throws -> Int64 { try decodeInt(Int64.self) }
    func decode(_ type: UInt.Type) throws -> UInt { try decodeInt(UInt.self) }
    func decode(_ type: UInt8.Type) throws -> UInt8 { try decodeInt(UInt8.self) }
    func decode(_ type: UInt16.Type) throws -> UInt16 { try decodeInt(UInt16.self) }
    func decode(_ type: UInt32.Type) throws -> UInt32 { try decodeInt(UInt32.self) }
    func decode(_ type: UInt64.Type) throws -> UInt64 { fatalError("UInt64 cannot be represented") }

    func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if T.self == URL.self { return try self.decode(URL.self) as! T }
        if T.self == Data.self { return try self.decode(Data.self) as! T }
        if T.self == Date.self { return try self.decode(Date.self) as! T }
        
        if let dataType = T.self as? CryoColumnDataValue.Type {
            guard let value = value as? CryoColumnDataValue else {
                throw DecodingError.typeMismatch(T.self, .init(codingPath: codingPath, debugDescription: "unexpected CryoPersistable: \(value)"))
            }
            
            return try dataType.init(dataValue: value.dataValue) as! T
        }
        
        return try T(from: CryoModelValueDecoder(value: value))
    }
}
