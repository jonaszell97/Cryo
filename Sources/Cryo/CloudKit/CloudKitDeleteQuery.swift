
import CloudKit
import Foundation

public final class CloudKitDeleteQuery<Model: CryoModel> {
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
            var result = "DELETE FROM \(Model.tableName)"
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

extension CloudKitDeleteQuery: CryoDeleteQuery {
    public func execute() async throws -> Int {
        let records = try await CloudKitSelectQuery<Model>.fetch(id: id, whereClauses: whereClauses, database: database)
        return try await withCheckedThrowingContinuation { continuation in
            let operation = CKModifyRecordsOperation()
            operation.recordIDsToDelete = records.map { $0.recordID }
            
            var deletedRecordCount: Int = 0
            operation.perRecordDeleteBlock = { id, result in
                guard case .success = result else {
                    return
                }
                
                deletedRecordCount += 1
            }
            
            operation.completionBlock = {
                continuation.resume(returning: deletedRecordCount)
            }
            
            database.add(operation)
        }
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
