
import CloudKit
import Foundation
import os

public final class CloudKitUpdateQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedCloudKitUpdateQuery
    
    /// Create an UPDATE query.
    internal init(from: Model.Type, id: String?, database: any CloudKitDatabase, config: CryoConfig?) throws {
        self.untypedQuery = try .init(for: Model.self, id: id, database: database, config: config)
    }
}

extension CloudKitUpdateQuery: CryoUpdateQuery {
    public var id: String? { untypedQuery.id }
    public var whereClauses: [CryoQueryWhereClause] { untypedQuery.whereClauses }
    public var setClauses: [CryoQuerySetClause] { untypedQuery.setClauses }
    
    public var queryString: String {
        untypedQuery.queryString
    }
    
    @discardableResult public func execute() async throws -> Int {
        try await untypedQuery.execute()
    }
    
    
    public func set<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        to value: Value
    ) throws -> Self {
        _ = try untypedQuery.set(columnName, to: value)
        return self
    }
    
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) throws -> Self {
        _ = try untypedQuery.where(columnName, operation: operation, value: value)
        return self
    }
}

internal class UntypedCloudKitUpdateQuery {
    /// The ID of the record to fetch.
    let id: String?
    
    /// The model type.
    let modelType: any CryoModel.Type
    
    /// The set clauses.
    var setClauses: [CryoQuerySetClause]
    
    /// The where clauses.
    var whereClauses: [CryoQueryWhereClause]
    
    /// The database to store to.
    let database: any CloudKitDatabase
    
    let config: CryoConfig?
    
    /// Create an UPDATE query.
    internal init(for modelType: any CryoModel.Type, id: String?, database: any CloudKitDatabase, config: CryoConfig?) throws {
        self.id = id
        self.database = database
        self.modelType = modelType
        self.setClauses = []
        self.whereClauses = []
        
        self.config = config
    }
    
    /// The complete query string.
    public var queryString: String {
        let hasId = id != nil
        var result = "UPDATE \(modelType.tableName)"
        
        // Set clauses
        
        for i in 0..<setClauses.count {
            if i == 0 {
                result += " SET "
            }
            else {
                result += ", "
            }
            
            let clause = setClauses[i]
            result += "\(clause.columnName) = \(CloudKitAdaptor.placeholderSymbol(for: clause.value))"
        }
        
        // Where clauses
        
        if hasId || !whereClauses.isEmpty {
            result += " WHERE "
        }
        
        if hasId {
            result += "id == %@"
        }
        
        for i in 0..<whereClauses.count {
            if i > 0 || hasId {
                result += " AND "
            }
            
            let clause = whereClauses[i]
            result += "\(clause.columnName) \(CloudKitAdaptor.formatOperator(clause.operation)) \(CloudKitAdaptor.placeholderSymbol(for: clause.value))"
        }
        
        return result
    }
}

extension UntypedCloudKitUpdateQuery {
    func fetch() async throws -> [CKRecord] {
        try await UntypedCloudKitSelectQuery.fetch(
            id: id, modelType: modelType, whereClauses: whereClauses,
            resultsLimit: nil, sortingClauses: [], database: database, log: config?.log
        )
    }
}

extension UntypedCloudKitUpdateQuery {
    @discardableResult public func execute() async throws -> Int {
        let records = try await self.fetch()
        guard !records.isEmpty else { return 0 }
        let schema = try CryoSchemaManager.shared.schema(for: modelType)
        for record in records {
            for clause in setClauses {
                guard let column = schema.columns.first(where: { $0.columnName == clause.columnName }) else {
                    throw CryoError.invalidModel(message: "Unknown column '\(clause.columnName)' in \(modelType.tableName)")
                }
                record[clause.columnName] = try CloudKitAdaptor.nsObject(from: clause.value.columnValue,
                                                                         column: column)
            }
        }
        
        var log: Optional<(OSLogType, String) -> Void> = nil
        
        #if DEBUG
        log = config?.log
        config?.log?(.debug, "[CloudKitAdaptor] \(queryString), SET \(setClauses.map { "\($0.value)" }), WHERE \(whereClauses.map { "\($0.value)" })")
        #endif
        
        let (saveResults, _) = try await CloudKitAdaptor.cloudKitOperation(log: log) {
            let results = try await database.modifyRecords(saving: records, deleting: [], savePolicy: .changedKeys)
            if let error = results.saveResults.values.compactMap({ result -> Error? in
                guard case .failure(let error) = result,
                      CloudKitAdaptor.isRetryableCloudKitError(error) else { return nil }
                return error
            }).first {
                throw error
            }
            return results
        }
        
        for result in saveResults {
            switch result.value {
            case .success(let id):
                #if DEBUG
                config?.log?(.debug, "[CloudKitAdaptor] Success! \(id)")
                #endif
            case .failure(let err):
                #if DEBUG
                config?.log?(.debug, "[CloudKitAdaptor] FAILURE: \(err.localizedDescription)")
                #endif
                throw err
            }
        }

        return saveResults.values.reduce(into: 0) { count, result in
            if case .success = result { count += 1 }
        }
    }
    
    public func set<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        to value: Value
    ) throws -> Self {
        self.setClauses.append(.init(columnName: columnName, value: try .init(value: value)))
        return self
    }
    
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) throws -> Self {
        self.whereClauses.append(.init(columnName: columnName,
                                       operation: operation,
                                       value: try .init(value: value)))
        
        return self
    }
}
