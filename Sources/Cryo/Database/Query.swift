
import Foundation

public enum CryoQueryResult<Model: CryoModel> {
    /// The operation completed successfully.
    case completed(affectedRows: Int)
    
    /// A number of records was fetched.
    case fetched(rows: [Model])
}

public enum CryoQueryValue {
    case string(value: String)
    case integer(value: Int)
    case double(value: Double)
    case date(value: Date)
    case data(value: Data)
    case asset(value: URL)
}

public protocol CryoQuery {
    /// The result type of this query.
    associatedtype Result
    
    /// The raw query string.
    var queryString: String { get async }
    
    /// Execute the query and return the result.
    func execute() async throws -> Result
}

public enum CryoComparisonOperator: String {
    /// Equality (==) comparison.
    case equals
    
    /// Inequality (!=) comparison.
    case doesNotEqual
    
    /// Greater than (>) comparison.
    case isGreatherThan
    
    /// Greater than or equals (>=) comparison.
    case isGreatherThanOrEquals
    
    /// Less than (<) comparison.
    case isLessThan
    
    /// Less than or equals (<=) comparison.
    case isLessThanOrEquals
}

public struct CryoQueryWhereClause {
    /// The name of the column.
    let columnName: String
    
    /// The operator.
    let operation: CryoComparisonOperator
    
    /// The value.
    let value: CryoQueryValue
}

public protocol CryoSelectQuery<Model>: CryoQuery where Self.Result == [Model] {
    /// The model type of the query.
    associatedtype Model: CryoModel
    
    /// Attach a WHERE clause to this query.
    @discardableResult func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) async throws -> Self
}

extension CryoSelectQuery {
    /// Attach a WHERE clause to this query.
    @discardableResult func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        equals value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: .equals, value: value)
    }
    
    /// Attach a WHERE clause to this query.
    @discardableResult func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        doesNotEqual value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: .doesNotEqual, value: value)
    }
    
    /// Attach a WHERE clause to this query.
    @discardableResult func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        isGreatherThan value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: .isGreatherThan, value: value)
    }
    
    /// Attach a WHERE clause to this query.
    @discardableResult func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        isGreatherThanOrEquals value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: .isGreatherThanOrEquals, value: value)
    }
    
    /// Attach a WHERE clause to this query.
    @discardableResult func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        isLessThan value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: .isLessThan, value: value)
    }
    
    /// Attach a WHERE clause to this query.
    @discardableResult func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        isLessThanOrEquals value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: .isLessThanOrEquals, value: value)
    }
}

// MARK: Conformances

extension CryoQueryValue: Codable {
    enum CodingKeys: String, CodingKey {
        case string, integer, double, date, data, asset
    }
    
    var codingKey: CodingKeys {
        switch self {
        case .string: return .string
        case .integer: return .integer
        case .double: return .double
        case .date: return .date
        case .data: return .data
        case .asset: return .asset
        }
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .string(let value):
            try container.encode(value, forKey: .string)
        case .integer(let value):
            try container.encode(value, forKey: .integer)
        case .double(let value):
            try container.encode(value, forKey: .double)
        case .date(let value):
            try container.encode(value, forKey: .date)
        case .data(let value):
            try container.encode(value, forKey: .data)
        case .asset(let value):
            try container.encode(value, forKey: .asset)
        }
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch container.allKeys.first {
        case .string:
            let value = try container.decode(String.self, forKey: .string)
            self = .string(value: value)
        case .integer:
            let value = try container.decode(Int.self, forKey: .integer)
            self = .integer(value: value)
        case .double:
            let value = try container.decode(Double.self, forKey: .double)
            self = .double(value: value)
        case .date:
            let value = try container.decode(Date.self, forKey: .date)
            self = .date(value: value)
        case .data:
            let value = try container.decode(Data.self, forKey: .data)
            self = .data(value: value)
        case .asset:
            let value = try container.decode(URL.self, forKey: .asset)
            self = .asset(value: value)
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

extension CryoQueryValue: Equatable {
    public static func ==(lhs: CryoQueryValue, rhs: CryoQueryValue) -> Bool {
        guard lhs.codingKey == rhs.codingKey else {
            return false
        }
        
        switch lhs {
        case .string(let value):
            guard case .string(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .integer(let value):
            guard case .integer(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .double(let value):
            guard case .double(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .date(let value):
            guard case .date(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .data(let value):
            guard case .data(let value_) = rhs else { return false }
            guard value == value_ else { return false }
        case .asset(let value):
            guard case .asset(let value_) = rhs else { return false }
            guard value == value_ else { return false }
            
        }
        
        return true
    }
}

extension CryoQueryValue: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.codingKey.rawValue)
        switch self {
        case .string(let value):
            hasher.combine(value)
        case .integer(let value):
            hasher.combine(value)
        case .double(let value):
            hasher.combine(value)
        case .date(let value):
            hasher.combine(value)
        case .data(let value):
            hasher.combine(value)
        case .asset(let value):
            hasher.combine(value)
            
        }
    }
}
