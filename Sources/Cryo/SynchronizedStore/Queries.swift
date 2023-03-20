
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
                                                value: Value) async throws -> Self {
        _ = try await query.set(columnName, value: value)
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
