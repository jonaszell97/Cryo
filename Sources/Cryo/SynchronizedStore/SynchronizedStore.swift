
import CloudKit
import Foundation

// MARK: SyncOperation

internal struct SyncOperation: CryoModel {
    static var tableName: String { "CryoSyncOperation" }
    
    /// The ID of this operation.
    @CryoColumn var id: String
    
    /// The ID of the store this operation was created by.
    @CryoColumn var storeIdentifier: String
    
    /// The device ID of the store this operation was created by.
    @CryoColumn var deviceIdentifier: String
    
    /// The date of the operation.
    @CryoColumn var date: Date
    
    /// The operation data.
    @CryoColumn var operationData: Data
    
    /// The operation.
    var operation: DatabaseOperation {
        get throws {
            try JSONDecoder().decode(DatabaseOperation.self, from: operationData)
        }
    }
    
    /// Create a sync operation.
    init(id: String = UUID().uuidString, storeIdentifier: String, deviceIdentifier: String, date: Date, operation: DatabaseOperation) throws {
        self.id = id
        self.storeIdentifier = storeIdentifier
        self.deviceIdentifier = deviceIdentifier
        self.date = date
        self.operationData = try JSONEncoder().encode(operation)
    }
}

// MARK: SynchronizedStoreConfig

public struct SynchronizedStoreConfig {
    /// The unique identifier of this store.
    public let storeIdentifier: String
    
    /// The URL of the local SQLite database.
    public let localDatabaseUrl: URL
    
    /// The CloudKit container identifier.
    public let containerIdentifier: String
    
    /// The models that are managed by this store.
    public let managedModels: [any CryoModel.Type]
    
    /// The cryo config.
    public let cryoConfig: CryoConfig
    
    /// Create a synchronized store config.
    public init(storeIdentifier: String,
                localDatabaseUrl: URL,
                containerIdentifier: String,
                managedModels: [CryoModel.Type],
                cryoConfig: CryoConfig) {
        self.storeIdentifier = storeIdentifier
        self.localDatabaseUrl = localDatabaseUrl
        self.containerIdentifier = containerIdentifier
        self.managedModels = managedModels
        self.cryoConfig = cryoConfig
    }
}

// MARK: SynchronizedStore

public final class SynchronizedStore {
    /// The store implementation.
    let store: SynchronizedStoreImpl<ResilientCloudKitStore>
    
    /// Create a synchronized store.
    public init(config: SynchronizedStoreConfig) async throws {
        let cloudKitStore = await CloudKitAdaptor(config: config.cryoConfig,
                                            containerIdentifier: config.containerIdentifier,
                                            database: \.privateCloudDatabase)
        
        let backend = try await ResilientCloudKitStore(store: cloudKitStore,
                                                       config: .init(identifier: "\(config.storeIdentifier)_resilient",
                                                                     cryoConfig: config.cryoConfig))
        
        self.store = try await SynchronizedStoreImpl(config: config, backend: backend)
    }
}

public extension SynchronizedStore {
    /// Needs to be invoked when a change notification was received.
    func externalChangeNotificationReceived(recordId: String? = nil) async throws {
        try await store.externalChangeNotificationReceived(recordId: recordId)
    }
}

// MARK: SynchronizedStoreImpl

internal final class SynchronizedStoreImpl<Backend: SynchronizedStoreBackend> {
    /// The configuration for this store.
    let config: SynchronizedStoreConfig
    
    /// The SQLite adaptor that accesses the local version of the database.
    let localStore: SQLiteAdaptor
    
    /// The adaptor responsible for fetching and persisting database operations.
    let operationsStore: Backend
    
    /// The device identifier for this device.
    let deviceIdentifier: String
    
    /// The date of the last modification to the store.
    @CryoKeyValue var lastModificationDate: Date
    
    /// The date of the last synchronization with the cloud store.
    @CryoKeyValue var lastSynchronizationDate: Date
    
    /// Whether the CloudKit subscription was setup.
    @CryoKeyValue var changeSubscriptionSetup: Bool
    
    /// Create a cloud-backed SQLite adaptor.
    init(config: SynchronizedStoreConfig,
         backend: Backend,
         deviceIdentifier: String? = nil) async throws {
        self.config = config
        self.localStore = try SQLiteAdaptor(databaseUrl: config.localDatabaseUrl)
        self.operationsStore = backend
        
        let deviceIdentifier = deviceIdentifier ?? Self.deviceIdentifier()
        self.deviceIdentifier = deviceIdentifier
        
        let modificationDateKey = "_csslm_\(config.storeIdentifier)~\(deviceIdentifier)"
        let synchronizationDateKey = "_cssls_\(config.storeIdentifier)~\(deviceIdentifier)"
        let subscriptionSetupKey = "_csssub_\(config.storeIdentifier)~\(deviceIdentifier)"
        
        self._lastModificationDate = .init(defaultValue: .distantPast, modificationDateKey)
        self._lastSynchronizationDate = .init(defaultValue: .distantPast, synchronizationDateKey)
        self._changeSubscriptionSetup = .init(defaultValue: false, subscriptionSetupKey)
        
        for modelType in config.managedModels {
            try await localStore.createTable(modelType: modelType).execute()
        }
        
        try await self.initialize()
    }
}

// MARK: Initialization

fileprivate extension SynchronizedStoreImpl {
    /// Initialize the store.
    func initialize() async throws {
        // Set up change subscription if necessary
        if !self.changeSubscriptionSetup {
            try await operationsStore.setupRecordChangeSubscription(for: SyncOperation.tableName,
                                                                    storeIdentifier: config.storeIdentifier,
                                                                    deviceIdentifier: deviceIdentifier) { recordId in
                try await self.externalChangeNotificationReceived(recordId: recordId)
            }
            
            self.changeSubscriptionSetup = true
        }
        
        // Perform initial synchronization
        try await self.externalChangeNotificationReceived()
    }
}

// MARK: External change synchronization

extension SynchronizedStoreImpl {
    /// Needs to be invoked when a change notification was received.
    func externalChangeNotificationReceived(recordId: String? = nil) async throws {
        let newOperations: [SyncOperation]
        if let id = recordId {
            guard let operation = try await operationsStore.loadOperation(withId: id) else {
                return
            }
            
            newOperations = [operation]
        }
        else {
            newOperations = try await operationsStore.loadOperations(after: self.lastSynchronizationDate,
                                                                     storeIdentifier: config.storeIdentifier,
                                                                     deviceIdentifier: deviceIdentifier)
            .sorted { $0.date < $1.date }
        }
        
        config.cryoConfig.log?(.debug, "[SynchronizedStore \(config.storeIdentifier)] found \(newOperations.count) new operations")
        
        for operation in newOperations {
            do {
                try await self.execute(operation: operation)
                self.lastSynchronizationDate = operation.date
                config.cryoConfig.log?(.debug, "[SynchronizedStore \(config.storeIdentifier)] successfully synchronized operation \(operation)")
            }
            catch {
                config.cryoConfig.log?(.error, "[SynchronizedStore \(config.storeIdentifier)] failed to synchronize operation \(operation)")
                
            }
        }
    }
    
    /// Execute a sync operation.
    func execute(operation: SyncOperation) async throws {
        try await localStore.execute(operation: try operation.operation)
    }
}

// MARK: Operations

fileprivate extension SynchronizedStoreImpl {
    /// Synchronize an INSERT operation that was executed locally.
    func didExecute<Model: CryoModel>(_ query: SQLiteInsertQuery<Model>) async throws {
        let operation = try await query.operation
        try await self.publish(operation: operation, tableName: Model.tableName)
    }
    
    /// Synchronize an UPDATE operation that was executed locally.
    func didExecute<Model: CryoModel>(_ query: SQLiteUpdateQuery<Model>) async throws {
        let operation = try await query.operation
        try await self.publish(operation: operation, tableName: Model.tableName)
    }
    
    /// Synchronize a DELETE operation that was executed locally.
    func didExecute<Model: CryoModel>(_ query: SQLiteDeleteQuery<Model>) async throws {
        let operation = try await query.operation
        try await self.publish(operation: operation, tableName: Model.tableName)
    }
    
    /// Publish an operation that was executed locally.
    func publish(operation: DatabaseOperation, tableName: String) async throws {
        guard (config.managedModels.contains { $0.tableName == tableName }) else {
            throw CryoError.schemaNotInitialized(tableName: tableName)
        }
        
        let syncOperation = try SyncOperation(storeIdentifier: config.storeIdentifier,
                                              deviceIdentifier: deviceIdentifier,
                                              date: .now, operation: operation)
        
        try await operationsStore.persist(operation: syncOperation)
    }
}

// MARK: Queries

extension SynchronizedStoreImpl: CryoDatabaseAdaptor {
    internal func createTable<Model: CryoModel>(for model: Model.Type) async throws -> any CryoCreateTableQuery<Model> {
        try await localStore.createTable(for: model)
    }
    
    internal func select<Model: CryoModel>(id: String? = nil, from model: Model.Type) async throws -> any CryoSelectQuery<Model> {
        try await localStore.select(id: id, from: model)
    }
    
    internal func insert<Model: CryoModel>(_ value: Model, replace: Bool = true)
        async throws -> SynchronizedInsertQuery<SQLiteInsertQuery<Model>>
    {
        let query = try await localStore.insert(value, replace: replace)
        return .init(query: query) {
            try await self.didExecute(query)
        }
    }
    
    internal func update<Model: CryoModel>(id: String? = nil, from modelType: Model.Type)
        async throws -> SynchronizedUpdateQuery<SQLiteUpdateQuery<Model>>
    {
        let query = try await localStore.update(id: id, from: modelType)
        return .init(query: query) {
            try await self.didExecute(query)
        }
    }
    
    internal func delete<Model: CryoModel>(id: String? = nil, from modelType: Model.Type)
        async throws -> SynchronizedDeleteQuery<SQLiteDeleteQuery<Model>>
    {
        let query = try await localStore.delete(id: id, from: modelType)
        return .init(query: query) {
            try await self.didExecute(query)
        }
    }
}

// MARK: Utility

fileprivate extension SynchronizedStoreImpl {
    static func deviceIdentifier() -> String {
        var systemInfo = utsname()
        uname(&systemInfo)
        
        let machineMirror = Mirror(reflecting: systemInfo.machine)
        let identifier = machineMirror.children.reduce("") { identifier, element in
            guard let value = element.value as? Int8, value != 0 else { return identifier }
            return identifier + String(UnicodeScalar(UInt8(value)))
        }
        
        return identifier
    }
}
