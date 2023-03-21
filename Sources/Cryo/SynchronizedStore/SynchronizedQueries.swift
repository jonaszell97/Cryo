
import Foundation

// MARK: Insert

public final class SynchronizedInsertQuery<QueryType: CryoInsertQuery> {
    /// The wrapped query.
    let query: QueryType
    
    /// Callback to invoke when the query is executed.
    let onExecutionCompleted: () async throws -> Void
    
    /// Create a synchronized select query.
    init(query: QueryType, onExecutionCompleted: @escaping () async throws -> Void) {
        self.query = query
        self.onExecutionCompleted = onExecutionCompleted
    }
}

extension SynchronizedInsertQuery: CryoInsertQuery {
    public var id: String { query.id }
    public var value: QueryType.Model { query.value }
    
    public typealias Model = QueryType.Model
    
    public var queryString: String {
        get async { await query.queryString }
    }
    
    @discardableResult public func execute() async throws -> QueryType.Result {
        let result = try await query.execute()
        try await onExecutionCompleted()
        
        return result
    }
}

// MARK: Update

public final class SynchronizedUpdateQuery<QueryType: CryoUpdateQuery> {
    /// The wrapped query.
    let query: QueryType
    
    /// Callback to invoke when the query is executed.
    let onExecutionCompleted: () async throws -> Void
    
    /// Create a synchronized select query.
    init(query: QueryType, onExecutionCompleted: @escaping () async throws -> Void) {
        self.query = query
        self.onExecutionCompleted = onExecutionCompleted
    }
}

extension SynchronizedUpdateQuery: CryoUpdateQuery {
    public var id: String? { query.id }
    public var whereClauses: [CryoQueryWhereClause] { query.whereClauses }
    public var setClauses: [CryoQuerySetClause] { query.setClauses }
    
    public typealias Model = QueryType.Model
    
    public var queryString: String {
        get async { await query.queryString }
    }
    
    @discardableResult public func execute() async throws -> QueryType.Result {
        let result = try await query.execute()
        try await onExecutionCompleted()
        
        return result
    }
    
    
    public func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                                    operation: CryoComparisonOperator,
                                                    value: Value) async throws -> Self {
        _ = try await query.where(columnName, operation: operation, value: value)
        return self
    }
    
    
    public func set<Value: _AnyCryoColumnValue>(_ columnName: String,
                                                to value: Value) async throws -> Self {
        _ = try await query.set(columnName, to: value)
        return self
    }
}

// MARK: Delete

public final class SynchronizedDeleteQuery<QueryType: CryoDeleteQuery> {
    /// The wrapped query.
    let query: QueryType
    
    /// Callback to invoke when the query is executed.
    let onExecutionCompleted: () async throws -> Void
    
    /// Create a synchronized select query.
    init(query: QueryType, onExecutionCompleted: @escaping () async throws -> Void) {
        self.query = query
        self.onExecutionCompleted = onExecutionCompleted
    }
}

extension SynchronizedDeleteQuery: CryoDeleteQuery {
    public var id: String? { query.id }
    public var whereClauses: [CryoQueryWhereClause] { query.whereClauses }
    
    public typealias Model = QueryType.Model
    
    public var queryString: String {
        get async { await query.queryString }
    }
    
    @discardableResult public func execute() async throws -> QueryType.Result {
        let result = try await query.execute()
        try await onExecutionCompleted()
        
        return result
    }
    
    
    public func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                                    operation: CryoComparisonOperator,
                                                    value: Value) async throws -> Self {
        _ = try await query.where(columnName, operation: operation, value: value)
        return self
    }
}
