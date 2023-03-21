
import CloudKit
@testable import Cryo

final class MockCloudKitAdaptor {
    /// The database to store to.
    var database: [String: [CKRecord.ID: CKRecord]]
    
    /// Cache of schema data.
    var schemas: [ObjectIdentifier: CryoSchema] = [:]
    
    /// Whether CloudKit is available.
    var isAvailable: Bool
    
    /// The change callbacks.
    var updateHooks: [String: [() async throws -> Void]] = [:]
    
    /// Default initializer.
    init() {
        self.database = [:]
        self.isAvailable = true
    }
    
    func getRecord(id: CKRecord.ID, tableName: String) -> CKRecord? {
        database[tableName]?[id]
    }
    
    func insertRecord(id: CKRecord.ID, record: CKRecord, tableName: String) {
        if database[tableName] == nil {
            database[tableName] = [:]
        }
        
        database[tableName]![id] = record
    }
    
    func allRecords(tableName: String) -> [CKRecord] {
        database[tableName]?.map { $0.value } ?? []
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
    let query: UntypedMockInsertQuery
    
    init(id: String, value: Model, updateHooks: @escaping () async throws -> Void,
         saveValue: @escaping (CKRecord.ID, CKRecord) -> Void) {
        self.query = .init(id: id, value: value, updateHooks: updateHooks, saveValue: saveValue)
    }
    
    @discardableResult func execute() async throws -> Bool {
        try await query.execute()
    }
    
    var queryString: String { "" }
    
    var id: String { query.id }
    var value: Model { query.value as! Model }
}

final class UntypedMockInsertQuery {
    let id: String
    let value: any CryoModel
    let saveValue: (CKRecord.ID, CKRecord) -> Void
    let updateHooks: () async throws -> Void
    
    init(id: String, value: any CryoModel,
         updateHooks: @escaping () async throws -> Void,
         saveValue: @escaping (CKRecord.ID, CKRecord) -> Void) {
        self.id = id
        self.value = value
        self.saveValue = saveValue
        self.updateHooks = updateHooks
    }
    
    var queryString: String { "" }
    
    @discardableResult func execute() async throws -> Bool {
        let id = CKRecord.ID(recordName: id)
        
        let modelType = type(of: value)
        let record = CKRecord(recordType: modelType.tableName, recordID: id)
        let schema = await CryoSchemaManager.shared.schema(for: modelType)
        
        for columnDetails in schema.columns {
            record[columnDetails.columnName] = try CloudKitAdaptor.nsObject(from: columnDetails.getValue(value),
                                                                            valueType: columnDetails.type)
        }
        
        saveValue(id, record)
        try await updateHooks()
        
        return true
    }
}
final class MockUpdateQuery<Model: CryoModel>: CryoUpdateQuery {
    let query: UntypedMockUpdateQuery
    
    init(from modelType: Model.Type,
         id: String?,
         allRecords: [CKRecord],
         updateHooks: @escaping () async throws -> Void,
         saveValue: @escaping (CKRecord.ID, CKRecord) -> Void) {
        self.query = .init(modelType: modelType, id: id, allRecords: allRecords,
                           updateHooks: updateHooks, saveValue: saveValue)
    }
    
    @discardableResult func execute() async throws -> Int {
        try await query.execute()
    }
    
    var queryString: String { "" }
    
    var id: String? { query.id }
    var whereClauses: [CryoQueryWhereClause] { query.whereClauses }
    var setClauses: [CryoQuerySetClause] { query.setClauses }
    
    func set<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        to value: Value
    ) async throws -> Self {
        _ = try await query.set(columnName, to: value)
        return self
    }
    
    func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                             operation: Cryo.CryoComparisonOperator,
                                             value: Value) async throws -> Self {
        _ = try await query.where(columnName, operation: operation, value: value)
        return self
    }
}

final class UntypedMockUpdateQuery {
    let id: String?
    let modelType: any CryoModel.Type
    let allRecords: [CKRecord]
    var setClauses: [CryoQuerySetClause]
    var whereClauses: [CryoQueryWhereClause]
    let saveValue: (CKRecord.ID, CKRecord) -> Void
    let updateHooks: () async throws -> Void
    
    init(modelType: any CryoModel.Type,
         id: String?,
         allRecords: [CKRecord],
         updateHooks: @escaping () async throws -> Void,
         saveValue: @escaping (CKRecord.ID, CKRecord) -> Void) {
        self.modelType = modelType
        self.id = id
        self.allRecords = allRecords
        self.saveValue = saveValue
        self.updateHooks = updateHooks
        self.setClauses = []
        self.whereClauses = []
    }
    
    var queryString: String { "" }
    
    func set<Value: _AnyCryoColumnValue>(
        _ columnName: String,
        to value: Value
    ) async throws -> Self {
        self.setClauses.append(.init(columnName: columnName, value: try .init(value: value)))
        return self
    }
    
    func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                             operation: Cryo.CryoComparisonOperator,
                                             value: Value) async throws
        -> Self
    {
        self.whereClauses.append(.init(columnName: columnName, operation: operation,
                                       value: try .init(value: value)))
        return self
    }
    
    @discardableResult func execute() async throws -> Int {
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
        
        try await updateHooks()
        return records.count
    }
}
final class MockDeleteQuery<Model: CryoModel>: CryoDeleteQuery {
    let query: UntypedMockDeleteQuery
    
    init(id: String?, allRecords: [CKRecord],
         updateHooks: @escaping () async throws -> Void,
         deleteRecord: @escaping (CKRecord.ID) -> Void) {
        self.query = .init(modelType: Model.self, id: id, allRecords: allRecords,
                           updateHooks: updateHooks, deleteRecord: deleteRecord)
    }
    
    @discardableResult func execute() async throws -> Int {
        try await query.execute()
    }
    
    var id: String? { query.id }
    var whereClauses: [CryoQueryWhereClause] { query.whereClauses }
    
    func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                             operation: Cryo.CryoComparisonOperator,
                                             value: Value) async throws -> Self {
        _ = try await query.where(columnName, operation: operation, value: value)
        return self
    }
    
    var queryString: String { "" }
    
    /// The database operation for this query.
    var operation: DatabaseOperation {
        get async throws {
            .delete(date: .now, tableName: Model.tableName, rowId: query.id, whereClauses: query.whereClauses)
        }
    }
}

final class UntypedMockDeleteQuery {
    let id: String?
    let modelType: any CryoModel.Type
    var whereClauses: [CryoQueryWhereClause]
    let allRecords: [CKRecord]
    let deleteRecord: (CKRecord.ID) -> Void
    let updateHooks: () async throws -> Void
    
    init(modelType: any CryoModel.Type,
         id: String?, allRecords: [CKRecord],
         updateHooks: @escaping () async throws -> Void,
         deleteRecord: @escaping (CKRecord.ID) -> Void) {
        self.id = id
        self.modelType = modelType
        self.allRecords = allRecords
        self.deleteRecord = deleteRecord
        self.updateHooks = updateHooks
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
        let schema = await CryoSchemaManager.shared.schema(for: modelType)
        
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
        
        try await updateHooks()
        return deletedCount
    }
}

extension MockCloudKitAdaptor: ResilientStoreBackend {
    func execute(operation: DatabaseOperation) async throws {
        switch operation {
        case .insert(_, let tableName, let rowId, let data):
            guard let schema = await CryoSchemaManager.shared.schema(tableName: tableName) else {
                throw CryoError.schemaNotInitialized(tableName: tableName)
            }
            
            var modelData = [String: _AnyCryoColumnValue]()
            for item in data {
                modelData[item.columnName] = item.value.columnValue
            }
            
            let model = try schema.create(modelData)
            let query = UntypedMockInsertQuery(id: rowId,
                                               value: model,
                                               updateHooks: { try await self.runUpdateHooks(tableName: tableName) }) { id, record in
                self.insertRecord(id: id, record: record, tableName: tableName)
            }
            
            _ = try await query.execute()
        case .update(_, let tableName, let rowId, let setClauses, let whereClauses):
            guard let schema = await CryoSchemaManager.shared.schema(tableName: tableName) else {
                throw CryoError.schemaNotInitialized(tableName: tableName)
            }
            
            let query = UntypedMockUpdateQuery(modelType: schema.`self`, id: rowId, allRecords: self.allRecords(tableName: tableName),
                                               updateHooks: { try await self.runUpdateHooks(tableName: tableName) }) { id, record in
                self.insertRecord(id: id, record: record, tableName: tableName)
            }
            
            for setClause in setClauses {
                _ = try await query.set(setClause.columnName, to: setClause.value.columnValue)
            }
            for whereClause in whereClauses {
                _ = try await query.where(whereClause.columnName, operation: whereClause.operation, value: whereClause.value.columnValue)
            }
            
            _ = try await query.execute()
            break
        case .delete(_, let tableName, let rowId, let whereClauses):
            guard let schema = await CryoSchemaManager.shared.schema(tableName: tableName) else {
                throw CryoError.schemaNotInitialized(tableName: tableName)
            }
            
            let query = UntypedMockDeleteQuery(modelType: schema.`self`, id: rowId, allRecords: self.allRecords(tableName: tableName),
                                               updateHooks: { try await self.runUpdateHooks(tableName: tableName) }) { id in
                self.database[tableName]?.removeValue(forKey: id)
            }
            
            for whereClause in whereClauses {
                _ = try await query.where(whereClause.columnName, operation: whereClause.operation, value: whereClause.value.columnValue)
            }
            
            _ = try await query.execute()
        }
    }
}

extension MockCloudKitAdaptor: CryoDatabaseAdaptor {
    public func createTable<Model: CryoModel>(for model: Model.Type) async throws -> any CryoCreateTableQuery<Model> {
        NoOpQuery(queryString: "", for: model)
    }
    
    public func select<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> any CryoSelectQuery<Model> {
        MockSelectQuery(id: id, allRecords: self.allRecords(tableName: Model.tableName))
    }
    
    public func insert<Model: CryoModel>(id: String = UUID().uuidString,
                                         _ value: Model,
                                         replace: Bool = true) async throws -> MockInsertQuery<Model> {
        MockInsertQuery(id: id, value: value,
                        updateHooks: { try await self.runUpdateHooks(tableName: Model.tableName) }) { id, record in
            self.insertRecord(id: id, record: record, tableName: Model.tableName)
        }
    }
    
    public func update<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> MockUpdateQuery<Model> {
        MockUpdateQuery(from: Model.self, id: id, allRecords: self.allRecords(tableName: Model.tableName),
                        updateHooks: { try await self.runUpdateHooks(tableName: Model.tableName) }) { id, record in
            self.insertRecord(id: id, record: record, tableName: Model.tableName)
        }
    }
    
    public func delete<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> MockDeleteQuery<Model> {
        MockDeleteQuery(id: id, allRecords: self.allRecords(tableName: Model.tableName),
                        updateHooks: { try await self.runUpdateHooks(tableName: Model.tableName) }) { id in
            self.database[Model.tableName]?.removeValue(forKey: id)
        }
    }
    
    func runUpdateHooks(tableName: String) async throws {
        guard let hooks = updateHooks[tableName] else {
            return
        }
        
        for hook in hooks {
            try await hook()
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

// MARK: Update hook

extension MockCloudKitAdaptor {
    /// Register a change callback.
    public func registerChangeListener<Model: CryoModel>(for modelType: Model.Type,
                                                         listener: @escaping () -> Void) {
        self.registerChangeListener(tableName: modelType.tableName, listener: listener)
    }
    
    /// Register a change callback.
    public func registerChangeListener(tableName: String, listener: @escaping () async throws -> Void) {
        if var hooks = updateHooks[tableName] {
            hooks.append(listener)
            updateHooks[tableName] = hooks
        }
        else {
            updateHooks[tableName] = [listener]
        }
    }
}

extension MockCloudKitAdaptor: SynchronizedStoreBackend {
    /// Persist a sync operation.
    internal func persist(operation: SyncOperation) async throws {
        try await self.insert(operation, replace: false).execute()
    }
    
    /// Load sync operations after a given date.
    internal func loadOperations(after date: Date,
                                 storeIdentifier: String,
                                 deviceIdentifier: String) async throws -> [SyncOperation] {
        try await self
            .select(from: SyncOperation.self)
            .where("date", isGreatherThan: date)
            .and("storeIdentifier", equals: storeIdentifier)
            .and("deviceIdentifier", doesNotEqual: deviceIdentifier)
            .execute()
    }
    
    /// Load a sync operation with the given ID.
    internal func loadOperation(withId id: String) async throws -> SyncOperation? {
        try await self.select(id: id, from: SyncOperation.self).execute().first
    }
    
    internal func setupRecordChangeSubscription(for tableName: String,
                                                storeIdentifier: String,
                                                deviceIdentifier: String,
                                                callback: @escaping (String?) async throws -> Void) async throws {
        self.registerChangeListener(tableName: tableName) {
            try await callback(nil)
        }
    }
}

extension ResilientStoreImpl where Backend == MockCloudKitAdaptor {
    func createTable<Model: CryoModel>(for model: Model.Type) async throws -> any CryoCreateTableQuery<Model> {
        try await store.createTable(for: model)
    }
    
    func select<Model: CryoModel>(id: String? = nil, from model: Model.Type) async throws -> any CryoSelectQuery<Model> {
        try await store.select(id: id, from: model)
    }
    
    func insert<Model: CryoModel>(id: String = UUID().uuidString,
                                  _ value: Model,
                                  replace: Bool = true) async throws -> ResilientInsertQuery<MockInsertQuery<Model>> {
        let query = try await store.insert(id: id, value, replace: replace)
        return ResilientInsertQuery(query: query) {
            try await self.enqueueFailedOperation(query.operation)
            return false
        }
    }
    
    func update<Model: CryoModel>(id: String? = nil, from modelType: Model.Type)
    async throws -> ResilientUpdateQuery<MockUpdateQuery<Model>>
    {
        let query = try await store.update(id: id, from: modelType)
        return ResilientUpdateQuery(query: query) {
            try await self.enqueueFailedOperation(query.operation)
            return 0
        }
    }
    
    func delete<Model: CryoModel>(id: String? = nil, from modelType: Model.Type)
    async throws -> ResilientDeleteQuery<MockDeleteQuery<Model>>
    {
        let query = try await store.delete(id: id, from: modelType)
        return ResilientDeleteQuery(query: query) {
            try await self.enqueueFailedOperation(query.operation)
            return 0
        }
    }
}
