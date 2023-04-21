
import CloudKit
import Foundation

public final class CloudKitSelectQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedCloudKitSelectQuery
    
    /// Create an UPDATE query.
    internal init(from: Model.Type, id: String?, database: CKDatabase, config: CryoConfig?) throws {
        self.untypedQuery = try .init(for: Model.self, id: id, database: database, config: config)
    }
}

extension CloudKitSelectQuery: CryoSelectQuery {
    public var id: String? { untypedQuery.id }
    public var whereClauses: [CryoQueryWhereClause] { untypedQuery.whereClauses }
    
    public var queryString: String {
        get async {
            await untypedQuery.queryString
        }
    }
    
    @discardableResult public func execute() async throws -> [Model] {
        try await untypedQuery.execute() as! [Model]
    }
    
    
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) async throws -> Self {
        _ = try await untypedQuery.where(columnName, operation: operation, value: value)
        return self
    }
    
    /// Limit the number of results this query returns.
    public func limit(_ limit: Int) -> Self {
        _ = untypedQuery.limit(limit)
        return self
    }
    
    /// Define a sorting for the results of this query.
    public func sort(by columnName: String, _ order: CryoSortingOrder) -> Self {
        _ = untypedQuery.sort(by: columnName, order)
        return self
    }
}

internal class UntypedCloudKitSelectQuery {
    /// The ID of the record to fetch.
    let id: String?
    
    /// The model type.
    let modelType: any CryoModel.Type
    
    /// The where clauses.
    var whereClauses: [CryoQueryWhereClause]
    
    /// The query results limit.
    var resultsLimit: Int? = nil
    
    /// The sorting clauses.
    var sortingClauses: [(String, CryoSortingOrder)] = []
    
    /// The database to store to.
    let database: CKDatabase
    
    #if DEBUG
    let config: CryoConfig?
    #endif
    
    /// Create a SELECT query.
    internal init(for modelType: any CryoModel.Type, id: String?, database: CKDatabase, config: CryoConfig?) throws {
        self.id = id
        self.database = database
        self.modelType = modelType
        self.whereClauses = []
        
        #if DEBUG
        self.config = config
        #endif
    }
    
    /// The complete query string.
    public var queryString: String {
        get async {
            var result = "SELECT * FROM \(modelType.tableName)"
            for i in 0..<whereClauses.count {
                if i == 0 {
                    result += " WHERE "
                }
                else {
                    result += " AND "
                }
                
                let clause = whereClauses[i]
                result += "\(clause.columnName) \(CloudKitAdaptor.formatOperator(clause.operation)) \(CloudKitAdaptor.placeholderSymbol(for: clause.value))"
            }
            
            return result
        }
    }
    
    /// Limit the number of results this query returns.
    public func limit(_ limit: Int) -> Self {
        self.resultsLimit = limit
        return self
    }
    
    /// Define a sorting for the results of this query.
    public func sort(by columnName: String, _ order: CryoSortingOrder) -> Self {
        self.sortingClauses.append((columnName, order))
        return self
    }
}

extension UntypedCloudKitSelectQuery {
    static func fetch(id: String?,
                      modelType: any CryoModel.Type,
                      whereClauses: [CryoQueryWhereClause],
                      resultsLimit: Int?,
                      sortingClauses: [(String, CryoSortingOrder)],
                      database: CKDatabase) async throws -> [CKRecord] {
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
        query.sortDescriptors = sortingClauses.map { .init(key: $0.0, ascending: $0.1 == .ascending) }
        
        var operation: CKQueryOperation? = CKQueryOperation(query: query)
        operation?.resultsLimit = resultsLimit ?? CKQueryOperation.maximumResults
        
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
                
                database.add(nextOperation)
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
    
    func decodeValue(from value: __CKRecordObjCValue, column: CryoSchemaColumn) async throws -> _AnyCryoColumnValue? {
        switch column {
        case .value(_, let type, _):
            return CloudKitAdaptor.decodeValue(from: value, as: type)
        case .oneToOneRelation(_, let modelType, _):
            let id = (value as! NSString) as String
            return try await UntypedCloudKitSelectQuery(for: modelType, id: id, database: database, config: config)
                .execute().first
        }
    }
}

extension UntypedCloudKitSelectQuery {
    public func execute() async throws -> [any CryoModel] {
        let records = try await Self.fetch(id: id, modelType: modelType, whereClauses: whereClauses,
                                           resultsLimit: resultsLimit, sortingClauses: sortingClauses,
                                           database: database)
        let schema = await CryoSchemaManager.shared.schema(for: modelType)
        
        var results = [any CryoModel]()
        for record in records {
            var data = [String: _AnyCryoColumnValue]()
            for columnDetails in schema.columns {
                guard
                    let object = record[columnDetails.columnName],
                    let value = try await self.decodeValue(from: object, column: columnDetails)
                else {
                    continue
                }
                
                data[columnDetails.columnName] = value
            }
            
            results.append(try schema.create(data))
        }
                          
        return results
    }
    
    /// Attach a WHERE clause to this query.
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) async throws -> Self {
        self.whereClauses.append(.init(columnName: columnName,
                                       operation: operation,
                                       value: try .init(value: value)))
        
        return self
    }
}
