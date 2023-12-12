
import Foundation

// MARK: Insert

public final class WrappedSelectQuery<QueryType: CryoSelectQuery> {
    /// The wrapped query.
    let query: QueryType
    
    /// Callback to invoke when the query is executed.
    let onExecutionCompleted: (QueryType.Result) async throws -> Void
    
    /// Create a Wrapped select query.
    init(query: QueryType, onExecutionCompleted: @escaping (QueryType.Result) async throws -> Void) {
        self.query = query
        self.onExecutionCompleted = onExecutionCompleted
    }
}

extension WrappedSelectQuery: CryoSelectQuery {
    public var id: String? { query.id }
    public func limit(_ limit: Int) -> Self {
        _ = query.limit(limit)
        return self
    }
    
    public func sort(by column: String, _ order: CryoSortingOrder) -> Self {
        _ = query.sort(by: column, order)
        return self
    }
    
    public var whereClauses: [CryoQueryWhereClause] {
        return query.whereClauses
    }
    
    public func `where`<Value>(_ columnName: String, operation: CryoComparisonOperator, 
                               value: Value) throws -> Self where Value : _AnyCryoColumnValue {
        _ = try query.where(columnName, operation: operation, value: value)
        return self
    }
    
    public typealias Model = QueryType.Model
    
    public var queryString: String {
        query.queryString
    }
    
    @discardableResult public func execute() async throws -> QueryType.Result {
        let result = try await query.execute()
        try await onExecutionCompleted(result)
        
        return result
    }
}

// MARK: Insert

public final class WrappedInsertQuery<QueryType: CryoInsertQuery> {
    /// The wrapped query.
    let query: QueryType
    
    /// Callback to invoke when the query is executed.
    let onExecutionCompleted: (QueryType.Result) async throws -> Void
    
    /// Create a Wrapped select query.
    init(query: QueryType, onExecutionCompleted: @escaping (QueryType.Result) async throws -> Void) {
        self.query = query
        self.onExecutionCompleted = onExecutionCompleted
    }
}

extension WrappedInsertQuery: CryoInsertQuery {
    public var id: String { query.id }
    public var value: QueryType.Model { query.value }
    
    public typealias Model = QueryType.Model
    
    public var queryString: String {
        query.queryString
    }
    
    @discardableResult public func execute() async throws -> QueryType.Result {
        let result = try await query.execute()
        try await onExecutionCompleted(result)
        
        return result
    }
}

// MARK: Update

public final class WrappedUpdateQuery<QueryType: CryoUpdateQuery> {
    /// The wrapped query.
    let query: QueryType
    
    /// Callback to invoke when the query is executed.
    let onExecutionCompleted: (QueryType.Result) async throws -> Void
    
    /// Create a Wrapped select query.
    init(query: QueryType, onExecutionCompleted: @escaping (QueryType.Result) async throws -> Void) {
        self.query = query
        self.onExecutionCompleted = onExecutionCompleted
    }
}

extension WrappedUpdateQuery: CryoUpdateQuery {
    public var id: String? { query.id }
    public var whereClauses: [CryoQueryWhereClause] { query.whereClauses }
    public var setClauses: [CryoQuerySetClause] { query.setClauses }
    
    public typealias Model = QueryType.Model
    
    public var queryString: String {
        query.queryString
    }
    
    @discardableResult public func execute() async throws -> QueryType.Result {
        let result = try await query.execute()
        try await onExecutionCompleted(result)
        
        return result
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

public final class WrappedDeleteQuery<QueryType: CryoDeleteQuery> {
    /// The wrapped query.
    let query: QueryType
    
    /// Callback to invoke when the query is executed.
    let onExecutionCompleted: (QueryType.Result) async throws -> Void
    
    /// Create a Wrapped select query.
    init(query: QueryType, onExecutionCompleted: @escaping (QueryType.Result) async throws -> Void) {
        self.query = query
        self.onExecutionCompleted = onExecutionCompleted
    }
}

extension WrappedDeleteQuery: CryoDeleteQuery {
    public var id: String? { query.id }
    public var whereClauses: [CryoQueryWhereClause] { query.whereClauses }
    
    public typealias Model = QueryType.Model
    
    public var queryString: String {
        query.queryString
    }
    
    @discardableResult public func execute() async throws -> QueryType.Result {
        let result = try await query.execute()
        try await onExecutionCompleted(result)
        
        return result
    }
    
    
    public func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                                    operation: CryoComparisonOperator,
                                                    value: Value) throws -> Self {
        _ = try query.where(columnName, operation: operation, value: value)
        return self
    }
}
