
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
    
    /// The name of the table this operation runs on.
    @CryoColumn var tableName: String
    
    /// The ID of the affected row.
    @CryoColumn var rowId: String
    
    /// The type of the operation.
    @CryoColumn var type: DatabaseOperationType
    
    /// The data required for this operation.
    @CryoColumn var data: [DatabaseOperationValue]
}

extension SyncOperation {
    /// Create a database operation from this sync operation.
    var databaseOperation: DatabaseOperation {
        .init(type: type, date: date, tableName: tableName, rowId: rowId, data: data)
    }
    
    /// Create a sync operation from a database operation.
    static func create(from operation: DatabaseOperation, storeIdentifier: String, deviceIdentifier: String) -> SyncOperation {
        .init(storeIdentifier: storeIdentifier, deviceIdentifier: deviceIdentifier,
              date: operation.date, tableName: operation.tableName, rowId: operation.rowId,
              type: operation.type, data: operation.data)
    }
}

// MARK: SynchronizedStoreConfig

public struct SynchronizedStoreConfig {
    /// The unique identifier of this store.
    public let storeIdentifier: String
    
    /// The database URL.
    public let localDatabaseUrl: URL
    
    /// The CloudKit container identifier.
    public let containerIdentifier: String
    
    /// The models that are managed by this store.
    public let managedModels: [CryoModel.Type]
    
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
    
    /// The adaptor for metadata.
    let metadataStore: UbiquitousKeyValueStoreAdaptor
    
    /// The date of the last modification to the store.
    @CryoKeyValue var lastModificationDate: Date
    
    /// The date of the last synchronization with the cloud store.
    @CryoUbiquitousKeyValue var lastSynchronizationDate: Date
    
    /// The device identifier for this device.
    let deviceIdentifier: String
    
    /// The current ubiquity token identifier.
    var ubiquityIdentityToken: String?
    
    /// The metadata change observer ID.
    var changeObserverId: ObjectIdentifier?
    
    /// The key for this device's metadata.
    let modificationDateKey: String
    
    /// The key for this device's metadata.
    let synchronizationDateKey: String
    
    /// Create a cloud-backed SQLite adaptor.
    public init(config: SynchronizedStoreConfig, cloudAdaptor: DocumentAdaptor) async throws {
        self.config = config
        self.localStore = try SQLiteAdaptor(databaseUrl: config.localDatabaseUrl)
        self.operationsStore = await CloudKitAdaptor(config: config.cryoConfig,
                                                     containerIdentifier: config.containerIdentifier,
                                                     database: \.privateCloudDatabase)
        self.metadataStore = .shared
        self.deviceIdentifier = Self.deviceIdentifier()
        self.ubiquityIdentityToken = nil
        self.changeObserverId = nil
        self.modificationDateKey = "_csslm_\(config.storeIdentifier)~\(deviceIdentifier)"
        self.synchronizationDateKey = "_cssls_\(config.storeIdentifier)~\(deviceIdentifier)"
        
        self._lastModificationDate = .init(wrappedValue: .distantPast, self.modificationDateKey)
        self._lastSynchronizationDate = .init(wrappedValue: .distantPast, self.synchronizationDateKey)
        
        try await self.initialize()
    }
    
    /// Remove the change observer.
    deinit {
        guard let changeObserverId else {
            return
        }
        
        metadataStore.removeObserver(withId: changeObserverId)
    }
}

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
    
    /// Whether a given key represents a sync metadata key for this store.
    func isSynchronizationMetadataKey(_ key: String) -> Bool {
        key.starts(with: "_csslm_\(config.storeIdentifier)~")
    }
    
    /// Update the ubiquity token.
    func updateUbiquityToken() throws {
        guard let token = FileManager.default.ubiquityIdentityToken as? Encodable else {
            self.ubiquityIdentityToken = nil
            config.cryoConfig.log?(.debug, "[SynchronizedStore \(config.storeIdentifier)] no ubiquity token")
            
            return
        }
        
        let encoder = JSONEncoder()
        let data = try encoder.encode(token)
        let string = String(data: data, encoding: .utf8)
        
        self.ubiquityIdentityToken = string
        config.cryoConfig.log?(.debug, "[SynchronizedStore \(config.storeIdentifier)] ubiquity token is \(string.debugDescription)")
    }
    
    /// Initialize the store.
    func initialize() async throws {
        // Update token
        try self.updateUbiquityToken()

        // Register change observer
        self.changeObserverId = metadataStore.observeChanges { change in
            self.config.cryoConfig.log?(.debug, "[SynchronizedStore \(self.config.storeIdentifier)] external change: \(change.reason.rawValue))")
            
            switch change.reason {
            case .initalSync:
                break
            case .accountChange:
                do {
                    try self.updateUbiquityToken()
                }
                catch {
                    self.config.cryoConfig.log?(.error, "[SynchronizedStore \(self.config.storeIdentifier)] updating ubiquity token failed: \(error.localizedDescription)")
                }
                break
            case .quotaViolation:
                break
            case .unknown:
                break
            case .dataChanged:
                Task {
                    do {
                        try await self.findChanges(changedKeys: change.changedKeys)
                    }
                    catch {
                        self.config.cryoConfig.log?(.error, "[SynchronizedStore \(self.config.storeIdentifier)] finding changes failed: \(error.localizedDescription)")
                    }
                }
            }
        }
        
        try await self.findChanges()
    }
    
    /// Check for external changes.
    func findChanges(changedKeys: [String]? = nil) async throws {
        config.cryoConfig.log?(.debug, "[SynchronizedStore \(config.storeIdentifier)] changed keys: \(changedKeys.debugDescription)")
        
        let allKeys = changedKeys ?? self.metadataStore.store.dictionaryRepresentation.keys.map { $0 }
        
        // Check the shared store for metadata changes
        
        var newestModificationDate: Date = .distantPast
        for stringKey in allKeys {
            guard isSynchronizationMetadataKey(stringKey) else {
                continue
            }
            
            let key = CryoNamedKey(id: stringKey, for: Date.self)
            guard let lastModificationDate = try await self.metadataStore.load(with: key) else {
                fatalError("found missing key \(stringKey)")
            }
            guard lastModificationDate > self.lastSynchronizationDate else {
                continue
            }
            
            newestModificationDate = max(newestModificationDate, lastModificationDate)
        }
        
        guard newestModificationDate > self.lastSynchronizationDate else {
            return
        }
        
        try await self.synchronizeExternalChanges()
    }
    
    /// Update the local database representation based on external changes.
    func synchronizeExternalChanges() async throws {
        // Fetch the changed records
        let predicate = NSPredicate(
            format: "date > %@ AND storeIdentifier == %@ AND deviceIdentifier != %@",
            NSDate(timeIntervalSinceReferenceDate: lastSynchronizationDate.timeIntervalSinceReferenceDate),
            config.storeIdentifier, deviceIdentifier)
        
        guard let newChanges = try await operationsStore.loadAll(of: SyncOperation.self, predicate: predicate) else {
            config.cryoConfig.log?(.debug, "[SynchronizedStore \(config.storeIdentifier)] failed to load new operations")
            return
        }
        
        for change in (newChanges.sorted { $0.date < $1.date }) {
            try await self.synchronizeChange(change: change)
            self.lastSynchronizationDate = change.date
        }
        
        config.cryoConfig.log?(.debug, "[SynchronizedStore \(config.storeIdentifier)] finished synchronising changes")
    }
    
    /// Handle a single external change.
    func synchronizeChange(change: SyncOperation) async throws {
        config.cryoConfig.log?(
            .debug,
            "[SynchronizedStore \(config.storeIdentifier)] synchronizing change of type \(change.type.rawValue) on table \(change.tableName)")
        
        let operation = change.databaseOperation
        try await localStore.execute(operation: operation)
    }
    
    /// Publish changes to this store.
    func publishChange(operation: DatabaseOperation) async throws {
        let key = CryoNamedKey(id: UUID().uuidString, for: SyncOperation.self)
        let change = SyncOperation.create(from: operation,
                                          storeIdentifier: config.storeIdentifier,
                                          deviceIdentifier: deviceIdentifier)
        
        try await operationsStore.persist(change, for: key)
    }
}

extension SynchronizedStore: CryoDatabaseAdaptor {
    public func createTable<Model: CryoModel>(for type: Model.Type) async throws -> any CryoQuery<Void> {
        try await localStore.createTable(for: type)
    }
    
    /// Create a SELECT by ID query.
    public func select<Model: CryoModel>(id: String?, from: Model.Type) async throws -> any CryoSelectQuery<Model> {
        fatalError()
    }
    
    public func insert<Model: CryoModel>(id: String, _ value: Model) async throws -> any CryoInsertQuery<Model> {
        fatalError("TODO")
    }
    
    public func update<Model: CryoModel>(id: String?) async throws -> any CryoUpdateQuery<Model> {
        fatalError("TODO")
    }
    
    public func delete<Model: CryoModel>(id: String?) async throws -> any CryoDeleteQuery<Model> {
        fatalError("TODO")
    }
    
    public func execute(operation: DatabaseOperation) async throws {
        // FIXME: What to do when one of these fails?
        try await localStore.execute(operation: operation)
        try await publishChange(operation: operation)
    }
    
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value?
        where Key.Value: CryoModel
    {
        try await localStore.load(with: key)
    }
    
    public func loadAll<Record: CryoModel>(of type: Record.Type) async throws -> [Record]? {
        try await localStore.loadAll(of: type)
    }
}
