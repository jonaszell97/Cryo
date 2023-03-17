
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

public protocol CryoQuery<Result> {
    /// The result type of this query.
    associatedtype Result
    
    /// The raw query string.
    var queryString: String { get async }
    
    /// Execute the query and return the result.
    func execute() async throws -> Result
}

public enum CryoComparisonOperator: String, Codable {
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

// MARK: Model query

public protocol CryoModelQuery<Model>: CryoQuery {
    /// The model type of the query.
    associatedtype Model: CryoModel
}

// MARK: No-op query

public struct NoOpQuery<Model: CryoModel>: CryoQuery {
    public typealias Result = Void
    
    public let queryString: String
    public func execute() async throws { }
    
    public init(queryString: String, for: Model.Type) {
        self.queryString = queryString
    }
}

// MARK: Multi-Query

public struct MultiQuery<Result1, Result2>: CryoQuery {
    /// The first query.
    let first: any CryoQuery<Result1>
    
    /// The second query.
    let second: any CryoQuery<Result2>
    
    public typealias Result = Void
    
    /// Create a multi query.
    public init(first: any CryoQuery<Result1>, second: any CryoQuery<Result2>) {
        self.first = first
        self.second = second
    }
    
    public var queryString: String {
        get async {
            "MultiQuery(\(await first.queryString), \(await second.queryString))"
        }
    }
    
    public func execute() async throws {
        _ = try await self.first.execute()
        _ = try await self.second.execute()
        
        return
    }
}

// MARK: WHERE clause

public struct CryoQueryWhereClause: Codable {
    /// The name of the column.
    let columnName: String
    
    /// The operator.
    let operation: CryoComparisonOperator
    
    /// The value.
    let value: CryoQueryValue
}

public protocol CryoWhereClauseQuery<Model>: CryoModelQuery {
    /// Attach a WHERE clause to this query.
    func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) async throws -> Self
}

// MARK: Set clauses

public struct CryoQuerySetClause: Codable {
    /// The name of the column.
    let columnName: String
    
    /// The value.
    let value: CryoQueryValue
}

public protocol CryoSetClauseQuery<Model>: CryoModelQuery {
    /// Attach a WHERE clause to this query.
    func set<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        value: Value
    ) async throws -> Self
}

// MARK: Select

public protocol CryoSelectQuery<Model>: CryoWhereClauseQuery
    where Self.Result == [Model]
{
    
}

// MARK: Insert

public protocol CryoInsertQuery<Model>: CryoModelQuery
    where Self.Result == Bool
{
    /// Execute the query and return the result.
    @discardableResult func execute() async throws -> Result
}

// MARK: Update

public protocol CryoUpdateQuery<Model>: CryoWhereClauseQuery, CryoSetClauseQuery
    where Self.Result == Int
{
    /// Execute the query and return the result.
    @discardableResult func execute() async throws -> Result
}

// MARK: Delete

public protocol CryoDeleteQuery<Model>: CryoWhereClauseQuery
    where Self.Result == Int
{
    /// Execute the query and return the result.
    @discardableResult func execute() async throws -> Result
}

// MARK: Utility extensions

extension CryoWhereClauseQuery {
    /// Attach an AND clause to this query.
    public func and<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: operation, value: value)
    }
    
    /// Attach a WHERE clause to this query.
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        equals value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: .equals, value: value)
    }
    
    /// Attach a WHERE clause to this query.
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        doesNotEqual value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: .doesNotEqual, value: value)
    }
    
    /// Attach a WHERE clause to this query.
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        isGreatherThan value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: .isGreatherThan, value: value)
    }
    
    /// Attach a WHERE clause to this query.
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        isGreatherThanOrEquals value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: .isGreatherThanOrEquals, value: value)
    }
    
    /// Attach a WHERE clause to this query.
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        isLessThan value: Value
    ) async throws -> Self {
        try await self.where(columnName, operation: .isLessThan, value: value)
    }
    
    /// Attach a WHERE clause to this query.
    public func `where`<Value: _AnyCryoColumnValue>(
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

internal extension CryoQueryValue {
    init (value: _AnyCryoColumnValue) throws {
        switch value {
        case let url as URL:
            self = .string(value: url.absoluteString)
        case let value as CryoColumnIntValue:
            self = .integer(value: Int(value.integerValue))
        case let value as CryoColumnDoubleValue:
            self = .double(value: value.doubleValue)
        case let value as CryoColumnStringValue:
            self = .string(value: value.stringValue)
        case let value as CryoColumnDateValue:
            self = .date(value: value.dateValue)
        case let value as CryoColumnDataValue:
            self = .data(value: try value.dataValue)
        default:
            self = .data(value: try JSONEncoder().encode(value))
        }
    }
    
    var columnValue: _AnyCryoColumnValue {
        switch self {
        case .string(let value):
            return value
        case .integer(let value):
            return value
        case .double(let value):
            return value
        case .date(let value):
            return value
        case .data(let value):
            return value
        case .asset(let value):
            return value
        }
    }
}
