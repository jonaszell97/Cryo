
import CloudKit
import Foundation

// MARK: SyncOperation

fileprivate struct SyncOperation: CryoModel {
    static var tableName: String { "CryoSyncOperation" }
    
    /// The ID of the store this operation was created by.
    @CryoColumn var storeIdentifier: String
    
    /// The device ID of the store this operation was created by.
    @CryoColumn var deviceIdentifier: String
    
    /// The date of the operation.
    @CryoColumn var date: Date
    
    /// The type of the operation.
    @CryoColumn var operation: DatabaseOperation
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
    /// The configuration for this store.
    let config: SynchronizedStoreConfig
    
    /// The SQLite adaptor that accesses the local version of the database.
    let localStore: SQLiteAdaptor
    
    /// The adaptor responsible for fetching and persisting database operations.
    let operationsStore: CloudKitAdaptor
    
    /// The device identifier for this device.
    let deviceIdentifier: String
    
    /// The date of the last modification to the store.
    @CryoKeyValue var lastModificationDate: Date
    
    /// The date of the last synchronization with the cloud store.
    @CryoKeyValue var lastSynchronizationDate: Date
    
    /// Whether the CloudKit subscription was setup.
    @CryoKeyValue var cloudKitSubscriptionSetup: Bool
    
    /// Create a cloud-backed SQLite adaptor.
    public init(config: SynchronizedStoreConfig, cloudAdaptor: DocumentAdaptor) async throws {
        self.config = config
        self.localStore = try SQLiteAdaptor(databaseUrl: config.localDatabaseUrl)
        self.operationsStore = await CloudKitAdaptor(config: config.cryoConfig,
                                                     containerIdentifier: config.containerIdentifier,
                                                     database: \.privateCloudDatabase)
        
        self.deviceIdentifier = Self.deviceIdentifier()
        
        let modificationDateKey = "_csslm_\(config.storeIdentifier)~\(deviceIdentifier)"
        let synchronizationDateKey = "_cssls_\(config.storeIdentifier)~\(deviceIdentifier)"
        let subscriptionSetupKey = "_csssub_\(config.storeIdentifier)~\(deviceIdentifier)"
        
        self._lastModificationDate = .init(defaultValue: .distantPast, modificationDateKey)
        self._lastSynchronizationDate = .init(defaultValue: .distantPast, synchronizationDateKey)
        self._cloudKitSubscriptionSetup = .init(defaultValue: false, subscriptionSetupKey)
        
        try await self.initialize()
    }
}

// MARK: Initialization

extension SynchronizedStore {
    /// Initialize the store.
    func initialize() async throws {
        // Set up CloudKit subscription if necessary
        if !self.cloudKitSubscriptionSetup {
            try await self.setupCloudKitSubscription()
            self.cloudKitSubscriptionSetup = true
        }
    }
}

// MARK: External change synchronization

extension SynchronizedStore {
    /// Set up the CloudKit subscription to be notified of changes coming from other devices.
    func setupCloudKitSubscription() async throws {
        // Subscribe to changes from other devices for this store
        let predicate = NSPredicate(format: "storeIdentifier == %@ AND deviceIdentifier != %@",
                                    config.storeIdentifier, self.deviceIdentifier)
        
        let subscription = CKQuerySubscription(recordType: SyncOperation.tableName,
                                               predicate: predicate,
                                               subscriptionID: "\(config.storeIdentifier)-operation-changes",
                                               options: .firesOnRecordCreation)
        
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo
        
        // Save the subscription
        let operation = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription],
                                                       subscriptionIDsToDelete: nil)
        
        return try await withCheckedThrowingContinuation { continuation in
            operation.modifySubscriptionsResultBlock = { result in
                if case .failure(let error) = result {
                    continuation.resume(throwing: error)
                    return
                }
            }
            
            operation.qualityOfService = .background
            CKContainer.default().privateCloudDatabase.add(operation)
        }
    }
    
    /// Callback invoked when a CloudKit notification is received.
    func receivedCloudKitNotification(_ notification: CKQueryNotification) async throws {
        guard case .recordCreated = notification.queryNotificationReason else {
            config.cryoConfig.log?(.debug, "received non-created notification: \(notification.queryNotificationReason.rawValue)")
            return
        }
        
        let query: any CryoSelectQuery<SyncOperation>
        if let id = notification.recordID {
            query = try await operationsStore
                .select(id: id.recordName, from: SyncOperation.self)
        }
        else {
            query = try await operationsStore
                .select(from: SyncOperation.self)
                .where("date", isGreatherThan: lastSynchronizationDate.timeIntervalSinceReferenceDate)
                .and("storeIdentifier", equals: config.storeIdentifier)
                .and("deviceIdentifier", doesNotEqual: deviceIdentifier)
        }
        
        let newOperations = try await query
            .execute()
            .sorted { $0.date < $1.date }
        
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
    fileprivate func execute(operation: SyncOperation) async throws {
        try await localStore.execute(operation: operation.operation)
    }
}

// MARK: Operations

extension SynchronizedStore {
    /// Synchronize an INSERT operation that was executed locally.
    func didExecute<Model: CryoModel>(_ query: SQLiteInsertQuery<Model>) async throws {
        let operation = try await query.operation
        try await self.publish(operation: operation)
    }
    
    /// Synchronize an UPDATE operation that was executed locally.
    func didExecute<Model: CryoModel>(_ query: SQLiteUpdateQuery<Model>) async throws {
        let operation = try await query.operation
        try await self.publish(operation: operation)
    }
    
    /// Synchronize a DELETE operation that was executed locally.
    func didExecute<Model: CryoModel>(_ query: SQLiteDeleteQuery<Model>) async throws {
        let operation = try await query.operation
        try await self.publish(operation: operation)
    }
    
    /// Publish an operation that was executed locally.
    func publish(operation: DatabaseOperation) async throws {
        let syncOperation = SyncOperation(storeIdentifier: config.storeIdentifier,
                                          deviceIdentifier: deviceIdentifier,
                                          date: .now, operation: operation)
        
        try await operationsStore.insert(syncOperation).execute()
    }
}

// MARK: Queries

extension SynchronizedStore: CryoDatabaseAdaptor {
    public func createTable<Model: CryoModel>(for model: Model.Type) async throws -> any CryoCreateTableQuery<Model> {
        try await localStore.createTable(for: model)
    }
    
    public func select<Model: CryoModel>(id: String? = nil, from model: Model.Type) async throws -> any CryoSelectQuery<Model> {
        try await localStore.select(id: id, from: model)
    }
    
    public func insert<Model: CryoModel>(id: String = UUID().uuidString, _ value: Model, replace: Bool = true)
        async throws -> SynchronizedInsertQuery<SQLiteInsertQuery<Model>>
    {
        let query = try await localStore.insert(id: id, value, replace: replace)
        return .init(query: query) {
            try await self.didExecute(query)
        }
    }
    
    public func update<Model: CryoModel>(id: String? = nil, from modelType: Model.Type)
        async throws -> SynchronizedUpdateQuery<SQLiteUpdateQuery<Model>>
    {
        let query = try await localStore.update(id: id, from: modelType)
        return .init(query: query) {
            try await self.didExecute(query)
        }
    }
    
    public func delete<Model: CryoModel>(id: String? = nil, from modelType: Model.Type)
        async throws -> SynchronizedDeleteQuery<SQLiteDeleteQuery<Model>>
    {
        let query = try await localStore.delete(id: id, from: modelType)
        return .init(query: query) {
            try await self.didExecute(query)
        }
    }
}

// MARK: Utility

fileprivate extension SynchronizedStore {
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
