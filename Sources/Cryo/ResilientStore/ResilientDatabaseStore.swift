
import CloudKit
import Foundation

internal protocol ResilientStoreBackend: SynchronizedStoreBackend, CryoDatabaseAdaptor {
    /// Execute a database operation.
    func execute(operation: DatabaseOperation) async throws
}

fileprivate struct QueuedOperation: Codable {
    /// The ID of this operation.
    let id: UUID
    
    /// The date of this operation.
    let date: Date
    
    /// The operation.
    let operation: DatabaseOperation
    
    /// The number of times this operation was tried to be executed.
    var numberOfAttempts: Int = 0
}

public struct ResilientCloudKitStoreConfig {
    /// The identifier of this store.
    let identifier: String
    
    /// The maximum number of retrys for a failed operation.
    let maximumNumberOfRetries: Int
    
    /// The cryo config.
    let cryoConfig: CryoConfig
    
    /// Create a configuration for a resilient CloudKit store.
    public init(identifier: String, maximumNumberOfRetries: Int = 5, cryoConfig: CryoConfig = .init()) {
        self.identifier = identifier
        self.maximumNumberOfRetries = maximumNumberOfRetries
        self.cryoConfig = cryoConfig
    }
}

public final class ResilientCloudKitStore {
    /// The store implementation.
    let store: ResilientStoreImpl<CloudKitAdaptor>
    
    /// Create a resilient CloudKit store.
    public init(store: CloudKitAdaptor, config: ResilientCloudKitStoreConfig) async throws {
        self.store = try await .init(store: store, config: config)
    }
}

extension ResilientCloudKitStore: SynchronizedStoreBackend {
    func persist(operation: SyncOperation) async throws {
        try await store.persist(operation: operation)
    }
    
    func loadOperations(after: Date, storeIdentifier: String, deviceIdentifier: String) async throws -> [SyncOperation] {
        try await store.loadOperations(after: after, storeIdentifier: storeIdentifier, deviceIdentifier: deviceIdentifier)
    }
    
    /// Delete all operations.
    func clearOperations() async throws {
        try await store.clearOperations()
    }
    
    func loadOperation(withId id: String) async throws -> SyncOperation? {
        try await store.loadOperation(withId: id)
    }
    
    func setupRecordChangeSubscription(for tableName: String,
                                       storeIdentifier: String,
                                       deviceIdentifier: String) async throws {
        try await store.setupRecordChangeSubscription(for: tableName, storeIdentifier: storeIdentifier,
                                                      deviceIdentifier: deviceIdentifier)
    }
    
    func registerExternalChangeNotificationListener(for tableName: String, storeIdentifier: String, 
                                                    deviceIdentifier: String, callback: @escaping (String?) async throws -> Void) async throws {
        try await store.registerExternalChangeNotificationListener(for: tableName, storeIdentifier: storeIdentifier,
                                                                   deviceIdentifier: deviceIdentifier, callback: callback)
    }
}

extension ResilientCloudKitStore {
    func createTable<Model: CryoModel>(for model: Model.Type) async throws -> any CryoCreateTableQuery<Model> {
        try await store.createTable(for: model)
    }
    
    func select<Model: CryoModel>(id: String? = nil, from model: Model.Type) async throws -> CloudKitSelectQuery<Model> {
        try await store.select(id: id, from: model)
    }
    
    func insert<Model: CryoModel>(_ value: Model,
                                  replace: Bool = true) async throws -> ResilientInsertQuery<CloudKitInsertQuery<Model>> {
        try await store.insert(value, replace: replace)
    }
    
    func update<Model: CryoModel>(id: String? = nil, from modelType: Model.Type)
        async throws -> ResilientUpdateQuery<CloudKitUpdateQuery<Model>>
    {
        try await store.update(id: id, from: modelType)
    }
    
    func delete<Model: CryoModel>(id: String? = nil, from modelType: Model.Type)
        async throws -> ResilientDeleteQuery<CloudKitDeleteQuery<Model>>
    {
        try await store.delete(id: id, from: modelType)
    }
}

final class ResilientStoreImpl<Backend: ResilientStoreBackend> {
    /// The underlying store.
    let store: Backend
    
    /// The configuration for this store.
    let config: ResilientCloudKitStoreConfig
    
    /// The local queue of failed operations.
    @CryoLocalDocument fileprivate var failedOperationsQueue: [QueuedOperation]
    
    /// Create a resilient cloud kit store.
    public init(store: Backend, config: ResilientCloudKitStoreConfig) async throws {
        self.store = store
        self.config = config
        self._failedOperationsQueue = .init(defaultValue: [], "_crcks_\(config.identifier)", saveOnWrite: false)
        
        try await self.executeFailedOperations()
    }
}

extension ResilientStoreImpl {
    /// Execute an operation.
    func execute(operation: DatabaseOperation, enqueueIfFailed: Bool) async -> Bool {
        do {
            try await store.execute(operation: operation)
            return true
        }
        catch {
        }
        
        guard enqueueIfFailed else {
            return false
        }
        
        let queuedOperation = QueuedOperation(id: UUID(), date: .now, operation: operation)
        self.failedOperationsQueue.append(queuedOperation)
        
        return false
    }
    
    /// Enqueue a failed operation.
    func enqueueFailedOperation(_ operation: DatabaseOperation) async throws {
        let queuedOperation = QueuedOperation(id: UUID(), date: .now, operation: operation)
        self.failedOperationsQueue.append(queuedOperation)
        try await self._failedOperationsQueue.persist()
    }
    
    /// Try to execute the failed operations in the queue.
    func executeFailedOperations() async throws {
        let queue = failedOperationsQueue.sorted { $0.date < $1.date }
        for operation in queue {
            var operation = operation
            config.cryoConfig.log?(.debug, "retrying \(operation.id)")
            
            let completed = await self.execute(operation: operation.operation, enqueueIfFailed: false)
            
            self.failedOperationsQueue.removeAll { $0.id == operation.id }
            try await self._failedOperationsQueue.persist()
            
            guard !completed else {
                continue
            }
            
            guard operation.numberOfAttempts < config.maximumNumberOfRetries else {
                config.cryoConfig.log?(
                    .debug, "discarding operation \(operation.id) after \(operation.numberOfAttempts) failed attempts")
                continue
            }
            
            operation.numberOfAttempts += 1
            
            self.failedOperationsQueue.append(operation)
            try await self._failedOperationsQueue.persist()
        }
    }
}

extension ResilientStoreImpl where Backend == CloudKitAdaptor {
    func select<Model: CryoModel>(id: String? = nil, from model: Model.Type) async throws -> CloudKitSelectQuery<Model> {
        try store.select(id: id, from: model)
    }
    
    func insert<Model: CryoModel>(_ value: Model,
                                  replace: Bool = true) async throws -> ResilientInsertQuery<CloudKitInsertQuery<Model>> {
        let query = try store.insert(value, replace: replace) as! CloudKitInsertQuery<Model>
        return ResilientInsertQuery(query: query) {
            try await self.enqueueFailedOperation(query.operation)
            return false
        }
    }
    
    func update<Model: CryoModel>(id: String? = nil, from modelType: Model.Type)
        async throws -> ResilientUpdateQuery<CloudKitUpdateQuery<Model>>
    {
        let query = try store.update(id: id, from: modelType) as! CloudKitUpdateQuery<Model>
        return ResilientUpdateQuery(query: query) {
            try await self.enqueueFailedOperation(query.operation)
            return 0
        }
    }
    
    func delete<Model: CryoModel>(id: String? = nil, from modelType: Model.Type)
        async throws -> ResilientDeleteQuery<CloudKitDeleteQuery<Model>>
    {
        let query = try store.delete(id: id, from: modelType) as! CloudKitDeleteQuery<Model>
        return ResilientDeleteQuery(query: query) {
            try await self.enqueueFailedOperation(query.operation)
            return 0
        }
    }
}

extension ResilientStoreImpl: SynchronizedStoreBackend {
    func createTable<Model: CryoModel>(for model: Model.Type) async throws -> any CryoCreateTableQuery<Model> {
        try await store.createTable(for: model)
    }
    
    func persist(operation: SyncOperation) async throws {
        try await store.persist(operation: operation)
    }
    
    func loadOperations(after: Date, storeIdentifier: String, deviceIdentifier: String) async throws -> [SyncOperation] {
        try await store.loadOperations(after: after, storeIdentifier: storeIdentifier, deviceIdentifier: deviceIdentifier)
    }
    
    func loadOperation(withId id: String) async throws -> SyncOperation? {
        try await store.loadOperation(withId: id)
    }
    
    /// Delete all operations.
    internal func clearOperations() async throws {
        try await store.clearOperations()
    }
    
    func setupRecordChangeSubscription(for tableName: String,
                                       storeIdentifier: String,
                                       deviceIdentifier: String) async throws {
        try await store.setupRecordChangeSubscription(for: tableName, storeIdentifier: storeIdentifier,
                                                      deviceIdentifier: deviceIdentifier)
    }
    
    func registerExternalChangeNotificationListener(for tableName: String, storeIdentifier: String,
                                                    deviceIdentifier: String, callback: @escaping (String?) async throws -> Void) async throws {
        try await store.registerExternalChangeNotificationListener(for: tableName, storeIdentifier: storeIdentifier,
                                                                   deviceIdentifier: deviceIdentifier, callback: callback)
    }
}
