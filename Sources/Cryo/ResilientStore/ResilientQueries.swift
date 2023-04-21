
import Foundation

// MARK: Insert

public final class ResilientInsertQuery<QueryType: CryoInsertQuery> {
    /// The wrapped query.
    let query: QueryType
    
    /// Callback to invoke when the query is executed.
    let onExecutionFailed: () async throws -> QueryType.Result
    
    /// Create a Resilient select query.
    init(query: QueryType, onExecutionFailed: @escaping () async throws -> QueryType.Result) {
        self.query = query
        self.onExecutionFailed = onExecutionFailed
    }
}

extension ResilientInsertQuery: CryoInsertQuery {
    public var id: String { query.id }
    public var value: QueryType.Model { query.value }
    
    public typealias Model = QueryType.Model
    
    public var queryString: String {
        get async { await query.queryString }
    }
    
    @discardableResult public func execute() async throws -> QueryType.Result {
        do {
            return try await query.execute()
        }
        catch {
            return try await onExecutionFailed()
        }
    }
}

// MARK: Update

public final class ResilientUpdateQuery<QueryType: CryoUpdateQuery> {
    /// The wrapped query.
    let query: QueryType
    
    /// Callback to invoke when the query is executed.
    let onExecutionFailed: () async throws -> QueryType.Result
    
    /// Create a Resilient select query.
    init(query: QueryType, onExecutionFailed: @escaping () async throws -> QueryType.Result) {
        self.query = query
        self.onExecutionFailed = onExecutionFailed
    }
}

extension ResilientUpdateQuery: CryoUpdateQuery {
    public var id: String? { query.id }
    public var whereClauses: [CryoQueryWhereClause] { query.whereClauses }
    public var setClauses: [CryoQuerySetClause] { query.setClauses }
    
    public typealias Model = QueryType.Model
    
    public var queryString: String {
        get async { await query.queryString }
    }
    
    @discardableResult public func execute() async throws -> QueryType.Result {
        do {
            return try await query.execute()
        }
        catch {
            return try await onExecutionFailed()
        }
    }
    
    public func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                                    operation: CryoComparisonOperator,
                                                    value: Value) throws -> Self {
        _ = try query.where(columnName, operation: operation, value: value)
        return self
    }
    
    public func set<Value: _AnyCryoColumnValue>(_ columnName: String,
                                                to value: Value) throws -> Self {
        _ = try query.set(columnName, to: value)
        return self
    }
}

// MARK: Delete

public final class ResilientDeleteQuery<QueryType: CryoDeleteQuery> {
    /// The wrapped query.
    let query: QueryType
    
    /// Callback to invoke when the query is executed.
    let onExecutionFailed: () async throws -> QueryType.Result
    
    /// Create a Resilient select query.
    init(query: QueryType, onExecutionFailed: @escaping () async throws -> QueryType.Result) {
        self.query = query
        self.onExecutionFailed = onExecutionFailed
    }
}

extension ResilientDeleteQuery: CryoDeleteQuery {
    public var id: String? { query.id }
    public var whereClauses: [CryoQueryWhereClause] { query.whereClauses }
    
    public typealias Model = QueryType.Model
    
    public var queryString: String {
        get async { await query.queryString }
    }
    
    @discardableResult public func execute() async throws -> QueryType.Result {
        do {
            return try await query.execute()
        }
        catch {
            return try await onExecutionFailed()
        }
    }
    
    public func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                                    operation: CryoComparisonOperator,
                                                    value: Value) throws -> Self {
        _ = try query.where(columnName, operation: operation, value: value)
        return self
    }
}
