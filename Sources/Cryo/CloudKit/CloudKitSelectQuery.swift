
import CloudKit
import Foundation
import os

public final class CloudKitSelectQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedCloudKitSelectQuery
    
    /// Create an UPDATE query.
    internal init(from: Model.Type, id: String?, database: any CloudKitDatabase, config: CryoConfig?) throws {
        self.untypedQuery = try .init(for: Model.self, id: id, database: database, config: config)
    }
}

extension CloudKitSelectQuery: CryoSelectQuery {
    public var id: String? { untypedQuery.id }
    public var whereClauses: [CryoQueryWhereClause] { untypedQuery.whereClauses }
    
    public var queryString: String {
        untypedQuery.queryString
    }
    
    @discardableResult public func execute() async throws -> [Model] {
        try await untypedQuery.execute() as! [Model]
    }
    
    
    public func `where`<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        operation: CryoComparisonOperator,
        value: Value
    ) throws -> Self {
        _ = try untypedQuery.where(columnName, operation: operation, value: value)
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
    let database: any CloudKitDatabase
    
    /// The cryo config.
    let config: CryoConfig?
    
    /// Create a SELECT query.
    internal init(for modelType: any CryoModel.Type, id: String?, database: any CloudKitDatabase, config: CryoConfig?) throws {
        self.id = id
        self.database = database
        self.modelType = modelType
        self.whereClauses = []
        self.config = config
    }
    
    /// The complete query string.
    public var queryString: String {
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
    static func makePredicate(id: String?, whereClauses: [CryoQueryWhereClause]) -> NSPredicate {
        // ID fetches bypass predicates, matching the existing query behavior.
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
                predicateFormat += "(%K \(CloudKitAdaptor.formatOperator(clause.operation)) %@)"
                predicateArgs.append(clause.columnName)
                predicateArgs.append(CloudKitAdaptor.queryArgument(for: clause.value))
            }

            predicate = NSPredicate(format: predicateFormat, argumentArray: predicateArgs)
        }

        return predicate
    }

    static func fetch(id: String?,
                      modelType: any CryoModel.Type,
                      whereClauses: [CryoQueryWhereClause],
                      resultsLimit: Int?,
                      sortingClauses: [(String, CryoSortingOrder)],
                      database: any CloudKitDatabase,
                      log: Optional<(OSLogType, String) -> Void> = nil
    ) async throws -> [CKRecord] {
        guard resultsLimit.map({ $0 > 0 }) ?? true else { return [] }

        if let id {
            let record: CKRecord
            do {
                record = try await CloudKitAdaptor.cloudKitOperation(log: log) {
                    try await database.record(for: .init(recordName: id))
                }
            } catch let error as CKError where error.code == .unknownItem {
                return []
            }

            let matches: Bool
            if whereClauses.isEmpty {
                matches = true
            } else {
                matches = try recordMatches(record, modelType: modelType, whereClauses: whereClauses)
            }
            guard matches else { return [] }
            return [record]
        }

        // Fetch all records matching WHERE clauses

        let predicate = makePredicate(id: id, whereClauses: whereClauses)

        let query = CKQuery(recordType: modelType.tableName, predicate: predicate)
        query.sortDescriptors = sortingClauses.map { .init(key: $0.0, ascending: $0.1 == .ascending) }
        
        var data = [CKRecord]()
        
        let firstLimit = resultsLimit ?? CKQueryOperation.maximumResults
        var (batch, cursor) = try await CloudKitAdaptor.cloudKitOperation(log: log) {
            try await database.records(matching: query, resultsLimit: firstLimit)
        }
        
        data.append(contentsOf: try batch.map { recordId, recordResult in
            switch recordResult {
            case .success(let record):
                return record
            case .failure(let error):
                throw error
            }
        })
        
        if let resultsLimit, data.count >= resultsLimit {
            return Array(data.prefix(resultsLimit))
        }

        while let currentCursor = cursor {
            let remaining = resultsLimit.map { max(1, $0 - data.count) } ?? CKQueryOperation.maximumResults
            let (nextBatch, nextCursor) = try await CloudKitAdaptor.cloudKitOperation(log: log) {
                try await database.records(continuingMatchFrom: currentCursor, resultsLimit: remaining)
            }
            
            data.append(contentsOf: try nextBatch.map { recordId, recordResult in
                switch recordResult {
                case .success(let record):
                    return record
                case .failure(let error):
                    throw error
                }
            })
            
            if let resultsLimit, data.count >= resultsLimit {
                break
            }
            
            cursor = nextCursor
        }
        
        return resultsLimit.map { Array(data.prefix($0)) } ?? data
    }

    private static func recordMatches(_ record: CKRecord,
                                      modelType: any CryoModel.Type,
                                      whereClauses: [CryoQueryWhereClause]) throws -> Bool {
        let schema = try CryoSchemaManager.shared.schema(for: modelType)
        for clause in whereClauses {
            guard let column = schema.columns.first(where: { $0.columnName == clause.columnName }) else {
                throw CryoError.invalidModel(message: "Unknown column '\(clause.columnName)' in \(modelType.tableName)")
            }
            let object: _AnyCryoColumnValue
            guard let rawValue = record[clause.columnName] else {
                object = Optional<String>.none
                if try !CloudKitAdaptor.check(clause: clause, object: object) { return false }
                continue
            }
            switch column {
            case .value(_, let type, _, _):
                guard let decoded = CloudKitAdaptor.decodeValue(from: rawValue, as: type) else {
                    throw CryoError.queryDecodeFailed(column: clause.columnName,
                                                      message: "Stored value has the wrong CloudKit type")
                }
                object = decoded.value
            case .oneToOneRelation:
                guard let relationID = rawValue as? NSString else {
                    throw CryoError.queryDecodeFailed(column: clause.columnName,
                                                      message: "Stored relation has no identifier")
                }
                object = relationID as String
            }
            if try !CloudKitAdaptor.check(clause: clause, object: object) { return false }
        }
        return true
    }
    
    func decodeValue(from value: __CKRecordObjCValue?, column: CryoSchemaColumn) async throws -> CryoColumnValueWrapper? {
        switch column {
        case .value(_, let type, let metaType, _):
            guard let value else {
                if let optionalType = metaType as? _CryoOptionalValue.Type {
                    return .init(value: optionalType.nilValue as! _AnyCryoColumnValue)
                }
                
                return nil
            }
            
            return CloudKitAdaptor.decodeValue(from: value, as: type)
        case .oneToOneRelation(_, let modelType, _):
            guard let idValue = value as? NSString else {
                throw CryoError.queryDecodeFailed(column: column.columnName,
                                                  message: "Missing or invalid relation identifier")
            }
            let id = idValue as String
            guard let result = try await UntypedCloudKitSelectQuery(for: modelType, id: id, database: database, config: config)
                .execute().first else {
                return nil
            }
            
            return CryoColumnValueWrapper(value: result)
        }
    }
}

extension UntypedCloudKitSelectQuery {
    public func execute() async throws -> [any CryoModel] {
        var log: Optional<(OSLogType, String) -> Void> = nil
        
        #if DEBUG
        log = config?.log
        config?.log?(.debug, "[CloudKitAdaptor] \(queryString), WHERE \(whereClauses.map { "\($0.value)" })")
        #endif
        
        let records = try await Self.fetch(id: id, modelType: modelType, whereClauses: whereClauses,
                                           resultsLimit: resultsLimit, sortingClauses: sortingClauses,
                                           database: database, log: log)
        
        let schema = try CryoSchemaManager.shared.schema(for: modelType)
        
        var results = [any CryoModel]()
        for record in records {
            var data = [String: CryoColumnValueWrapper]()
            for columnDetails in schema.columns {
                guard let value = try await self.decodeValue(from: record[columnDetails.columnName], column: columnDetails)
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
    ) throws -> Self {
        self.whereClauses.append(.init(columnName: columnName,
                                       operation: operation,
                                       value: try .init(value: value)))
        
        return self
    }
}
