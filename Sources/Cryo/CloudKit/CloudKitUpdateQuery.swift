
import CloudKit
import Foundation

public final class CloudKitUpdateQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedCloudKitUpdateQuery
    
    /// Create an UPDATE query.
    internal init(from: Model.Type, id: String?, database: CKDatabase, config: CryoConfig?) throws {
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
    let database: CKDatabase
    
    #if DEBUG
    let config: CryoConfig?
    #endif
    
    /// Create an UPDATE query.
    internal init(for modelType: any CryoModel.Type, id: String?, database: CKDatabase, config: CryoConfig?) throws {
        self.id = id
        self.database = database
        self.modelType = modelType
        self.setClauses = []
        self.whereClauses = []
        
        #if DEBUG
        self.config = config
        #endif
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
        if let id {
            // Fetch single record
            
            let recordId = CKRecord.ID(recordName: id)
            return try await withCheckedThrowingContinuation { continuation in
                database.fetch(withRecordID: recordId) { record, error in
                    if let error {
                        continuation.resume(throwing: error)
                        return
                    }
                    
                    guard let record else {
                        continuation.resume(returning: [])
                        return
                    }
                    
                    continuation.resume(returning: [record])
                }
            }
        }
        
        // Fetch all records matching WHERE clauses
        
        let predicate: NSPredicate
        if whereClauses.isEmpty {
            predicate = NSPredicate(value: true)
        }
        else {
            var predicateFormat = ""
            var predicateArgs = [Any]()
            
            for i in 0..<whereClauses.count {
                if i > 0 {
                    predicateFormat += " AND "
                }
                
                let clause = whereClauses[i]
                predicateFormat += "(\(clause.columnName) \(CloudKitAdaptor.formatOperator(clause.operation)) \(CloudKitAdaptor.placeholderSymbol(for: clause.value)))"
                predicateArgs.append(CloudKitAdaptor.queryArgument(for: clause.value))
            }
            
            predicate = NSPredicate(format: predicateFormat, argumentArray: predicateArgs)
        }
        
        let query = CKQuery(recordType: modelType.tableName, predicate: predicate)
        
        var operation: CKQueryOperation? = CKQueryOperation(query: query)
        operation?.resultsLimit = CKQueryOperation.maximumResults
        
        var data = [CKRecord]()
        while let nextOperation = operation {
            var encounteredError = false
            
            let cursor: CKQueryOperation.Cursor? = try await withCheckedThrowingContinuation { continuation in
                nextOperation.queryResultBlock =  {
                    guard !encounteredError else { return }
                    continuation.resume(with: $0)
                }
                nextOperation.recordMatchedBlock = { _, result in
                    switch result {
                    case .success(let record):
                        data.append(record)
                    case .failure(let error):
                        encounteredError = true
                        continuation.resume(throwing: error)
                    }
                }
                
                self.database.add(nextOperation)
            }
            
            if let cursor = cursor {
                operation = CKQueryOperation(cursor: cursor)
            }
            else {
                break
            }
        }
        
        return data
    }
}

extension UntypedCloudKitUpdateQuery {
    @discardableResult public func execute() async throws -> Int {
        let records = try await self.fetch()
        for record in records {
            for clause in setClauses {
                record[clause.columnName] = clause.value.recordValue
            }
        }
        
        return try await withCheckedThrowingContinuation { continuation in
            let operation = CKModifyRecordsOperation()
            operation.recordsToSave = records
            operation.savePolicy = .allKeys
            
            var savedRecordCount = 0
            operation.perRecordSaveBlock = { id, result in
                guard case .success = result else {
                    return
                }
                
                savedRecordCount += 1
            }
            
            
            operation.completionBlock = {
                continuation.resume(returning: savedRecordCount)
            }
            
            database.add(operation)
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
