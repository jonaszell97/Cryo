
import CloudKit
@testable import Cryo

final class MockCloudKitAdaptor {
    /// The database to store to.
    var database: [CKRecord.ID: CKRecord]
    
    /// Cache of schema data.
    var schemas: [ObjectIdentifier: CryoSchema] = [:]
    
    /// Whether CloudKit is available.
    var isAvailable: Bool
    
    /// Default initializer.
    init() {
        self.database = [:]
        self.isAvailable = true
    }
}

final class MockSelectQuery<Model: CryoModel>: CryoSelectQuery {
    let id: String?
    var whereClauses: [CryoQueryWhereClause]
    let allRecords: [CKRecord]
    
    init(id: String?, allRecords: [CKRecord]) {
        self.id = id
        self.allRecords = allRecords
        self.whereClauses = []
    }
    
    var queryString: String { "" }
    
    func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                             operation: Cryo.CryoComparisonOperator,
                                             value: Value) async throws
        -> MockSelectQuery<Model>
    {
        self.whereClauses.append(.init(columnName: columnName, operation: operation,
                                       value: try .init(value: value)))
        return self
    }
    
    func execute() async throws -> [Model] {
        let schema = await CryoSchemaManager.shared.schema(for: Model.self)
        
        var results = [Model]()
        for record in allRecords {
            if let id {
                guard record.recordID.recordName == id else {
                    continue
                }
            }
            
            var matches = true
            var data = [String: _AnyCryoColumnValue]()
            
            for columnDetails in schema.columns {
                guard
                    let object = record[columnDetails.columnName],
                    let value = CloudKitAdaptor.decodeValue(from: object, as: columnDetails.type)
                else {
                    continue
                }
                
                for clause in (whereClauses.filter { $0.columnName == columnDetails.columnName }) {
                    guard try CloudKitAdaptor.check(clause: clause, object: value) else {
                        matches = false
                        break
                    }
                }
                
                data[columnDetails.columnName] = value
            }
            
            guard matches else {
                continue
            }
            
            results.append(try Model(from: CryoModelDecoder(data: data)))
        }
        
        return results
    }
}

final class MockInsertQuery<Model: CryoModel>: CryoInsertQuery {
    let id: String
    let value: Model
    let saveValue: (CKRecord.ID, CKRecord) -> Void
    
    init(id: String, value: Model, saveValue: @escaping (CKRecord.ID, CKRecord) -> Void) {
        self.id = id
        self.value = value
        self.saveValue = saveValue
    }
    
    var queryString: String { "" }
    
    @discardableResult func execute() async throws -> Bool {
        let id = CKRecord.ID(recordName: id)
        
        let modelType = Model.self
        let record = CKRecord(recordType: modelType.tableName, recordID: id)
        let schema = await CryoSchemaManager.shared.schema(for: modelType)
        
        for columnDetails in schema.columns {
            record[columnDetails.columnName] = try CloudKitAdaptor.nsObject(from: columnDetails.getValue(value),
                                                                            valueType: columnDetails.type)
        }
        
        saveValue(id, record)
        return true
    }
}

final class MockUpdateQuery<Model: CryoModel>: CryoUpdateQuery {
    let id: String?
    let allRecords: [CKRecord]
    var setClauses: [CryoQuerySetClause]
    var whereClauses: [CryoQueryWhereClause]
    let saveValue: (CKRecord.ID, CKRecord) -> Void
    
    init(id: String?,
         allRecords: [CKRecord],
         saveValue: @escaping (CKRecord.ID, CKRecord) -> Void) {
        self.id = id
        self.allRecords = allRecords
        self.saveValue = saveValue
        self.setClauses = []
        self.whereClauses = []
    }
    
    var queryString: String { "" }
    
    func set<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        value: Value
    ) async throws -> Self {
        self.setClauses.append(.init(columnName: columnName, value: try .init(value: value)))
        return self
    }
    
    func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                             operation: Cryo.CryoComparisonOperator,
                                             value: Value) async throws
        -> MockUpdateQuery<Model>
    {
        self.whereClauses.append(.init(columnName: columnName, operation: operation,
                                       value: try .init(value: value)))
        return self
    }
    
    @discardableResult func execute() async throws -> Int {
        let modelType = Model.self
        let schema = await CryoSchemaManager.shared.schema(for: modelType)
        
        var records = [CKRecord]()
        for record in allRecords {
            if let id {
                guard record.recordID.recordName == id else {
                    continue
                }
            }
            
            var matches = true
            for columnDetails in schema.columns {
                guard
                    let object = record[columnDetails.columnName],
                    let value = CloudKitAdaptor.decodeValue(from: object, as: columnDetails.type)
                else {
                    continue
                }
                
                for clause in (whereClauses.filter { $0.columnName == columnDetails.columnName }) {
                    guard try CloudKitAdaptor.check(clause: clause, object: value) else {
                        matches = false
                        break
                    }
                }
            }
            
            guard matches else {
                continue
            }
            
            records.append(record)
        }
        
        for record in records {
            for setClause in setClauses {
                record[setClause.columnName] = setClause.value.recordValue
            }
            
            saveValue(record.recordID, record)
        }
        
        return records.count
    }
}

final class MockDeleteQuery<Model: CryoModel>: CryoDeleteQuery {
    let id: String?
    var whereClauses: [CryoQueryWhereClause]
    let allRecords: [CKRecord]
    let deleteRecord: (CKRecord.ID) -> Void
    
    init(id: String?, allRecords: [CKRecord], deleteRecord: @escaping (CKRecord.ID) -> Void) {
        self.id = id
        self.allRecords = allRecords
        self.deleteRecord = deleteRecord
        self.whereClauses = []
    }
    
    var queryString: String { "" }
    
    func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                             operation: Cryo.CryoComparisonOperator,
                                             value: Value) async throws -> Self
    {
        self.whereClauses.append(.init(columnName: columnName, operation: operation,
                                       value: try .init(value: value)))
        return self
    }
    
    @discardableResult func execute() async throws -> Int {
        let schema = await CryoSchemaManager.shared.schema(for: Model.self)
        
        var deletedCount = 0
        for record in allRecords {
            if let id {
                guard record.recordID.recordName == id else {
                    continue
                }
            }
            
            var matches = true
            for columnDetails in schema.columns {
                guard
                    let object = record[columnDetails.columnName],
                    let value = CloudKitAdaptor.decodeValue(from: object, as: columnDetails.type)
                else {
                    continue
                }
                
                for clause in (whereClauses.filter { $0.columnName == columnDetails.columnName }) {
                    guard try CloudKitAdaptor.check(clause: clause, object: value) else {
                        matches = false
                        break
                    }
                }
            }
            
            guard matches else {
                continue
            }
            
            deleteRecord(record.recordID)
            deletedCount += 1
        }
        
        return deletedCount
    }
}

extension MockCloudKitAdaptor: CryoDatabaseAdaptor {
    public func createTable<Model: CryoModel>(for model: Model.Type) async throws -> any CryoCreateTableQuery<Model> {
        NoOpQuery(queryString: "", for: model)
    }
    
    public func select<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> any CryoSelectQuery<Model> {
        MockSelectQuery(id: id, allRecords: self.database.values.map { $0 })
    }
    
    public func insert<Model: CryoModel>(id: String, _ value: Model, replace: Bool = true) async throws -> MockInsertQuery<Model> {
        MockInsertQuery(id: id, value: value) { id, record in
            self.database[id] = record
        }
    }
    
    public func update<Model: CryoModel>(id: String? = nil) async throws -> MockUpdateQuery<Model> {
        MockUpdateQuery(id: id, allRecords: self.database.values.map { $0 }) { id, record in
            self.database[id] = record
        }
    }
    
    public func delete<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> MockDeleteQuery<Model> {
        MockDeleteQuery(id: id, allRecords: self.database.values.map { $0 }) { id in
            self.database[id] = nil
        }
    }
}

extension MockCloudKitAdaptor {
    public func ensureAvailability() async throws {
        guard !isAvailable else {
            return
        }
        
        throw CryoError.backendNotAvailable
    }
    
    public func observeAvailabilityChanges(_ callback: @escaping (Bool) -> Void) {
        
    }
}
