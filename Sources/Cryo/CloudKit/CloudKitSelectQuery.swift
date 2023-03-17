
import CloudKit
import Foundation

public final class CloudKitSelectQuery<Model: CryoModel> {
    /// The ID of the record to fetch.
    let id: String?
    
    /// The where clauses.
    var whereClauses: [CryoQueryWhereClause]
    
    /// The database to store to.
    let database: CKDatabase
    
    #if DEBUG
    let config: CryoConfig?
    #endif
    
    /// Create a SELECT query.
    internal init(for: Model.Type, id: String?, database: CKDatabase, config: CryoConfig?) throws {
        self.id = id
        self.database = database
        self.whereClauses = []
        
        #if DEBUG
        self.config = config
        #endif
    }
    
    /// The complete query string.
    public var queryString: String {
        get async {
            var result = "SELECT * FROM \(Model.tableName)"
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
}

extension CloudKitSelectQuery {
    static func fetch(id: String?, whereClauses: [CryoQueryWhereClause], database: CKDatabase) async throws -> [CKRecord] {
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
        
        let query = CKQuery(recordType: Model.tableName, predicate: predicate)
        
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
}

extension CloudKitSelectQuery: CryoSelectQuery {
    public func execute() async throws -> [Model] {
        let records = try await Self.fetch(id: id, whereClauses: whereClauses, database: database)
        let schema = await CryoSchemaManager.shared.schema(for: Model.self)
        
        var results = [Model]()
        for record in records {
            var data = [String: _AnyCryoColumnValue]()
            for columnDetails in schema {
                guard
                    let object = record[columnDetails.columnName],
                    let value = CloudKitAdaptor.decodeValue(from: object, as: columnDetails.type)
                else {
                    continue
                }
                
                data[columnDetails.columnName] = value
            }
            
            results.append(try Model(from: CryoModelDecoder(data: data)))
        }
                          
        return results
    }
    
    /// Attach a WHERE clause to this query.
    @discardableResult public func `where`<Value: _AnyCryoColumnValue>(
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
