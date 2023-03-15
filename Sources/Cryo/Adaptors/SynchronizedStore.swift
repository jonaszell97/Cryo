
import Foundation

fileprivate struct SynchronizationMetadata {
    /// The date of the last modification to the store.
    let lastModificationDate: Date
    
    /// The file name of this store's database file.
    let databaseFilename: String
    
    /// The current ubiquity token identifier.
    let ubiquityIdentityToken: String?
}

public struct SynchronizedStoreConfig {
    /// The unique identifier of this store.
    public let storeIdentifier: String
    
    /// The database URL.
    public let databaseUrl: URL
    
    /// The models that are managed by this store.
    public let managedModels: [CryoModel.Type]
    
    /// The cryo config.
    public let cryoConfig: CryoConfig
    
    /// Create a synchronized store config.
    public init(storeIdentifier: String,
                databaseUrl: URL,
                managedModels: [CryoModel.Type],
                cryoConfig: CryoConfig) {
        self.storeIdentifier = storeIdentifier
        self.databaseUrl = databaseUrl
        self.managedModels = managedModels
        self.cryoConfig = cryoConfig
    }
}

public final class SynchronizedStore {
    /// The configuration for this store.
    let config: SynchronizedStoreConfig
    
    /// The adaptor responsible for storing the iCloud version of the database.
    let cloudStore: DocumentAdaptor?
    
    /// The SQLite adaptor that accesses the local version of the database.
    let sqliteStore: SQLiteAdaptor
    
    /// The adaptor for metadata.
    let metadataStore: UbiquitousKeyValueStoreAdaptor
    
    /// The date of the last modification to the store.
    var lastModificationDate: Date
    
    /// The device identifier for this device.
    let deviceIdentifier: String
    
    /// The current ubiquity token identifier.
    var ubiquityIdentityToken: String?
    
    /// The metadata change observer ID.
    var changeObserverId: ObjectIdentifier?
    
    /// Create a cloud-backed SQLite adaptor.
    public init(config: SynchronizedStoreConfig, cloudAdaptor: DocumentAdaptor) async throws {
        self.config = config
        self.sqliteStore = try SQLiteAdaptor(databaseUrl: config.databaseUrl)
        self.cloudStore = await DocumentAdaptor.cloud(fileManager: .default)
        self.metadataStore = .shared
        self.deviceIdentifier = Self.deviceIdentifier()
        self.ubiquityIdentityToken = nil
        self.lastModificationDate = .distantPast
        self.changeObserverId = nil
        
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
        
        // Load metadata
        
        let metadataKey = SynchronizationMetadataKey(storeIdentifier: config.storeIdentifier,
                                                     deviceIdentifier: deviceIdentifier)
        guard let metadata = try await metadataStore.load(with: metadataKey) else {
            try await self.findChanges()
            return
        }
        
        self.lastModificationDate = metadata.lastModificationDate
        try await self.findChanges()
    }
    
    /// Check for external changes.
    func findChanges(changedKeys: [String]? = nil) async throws {
        config.cryoConfig.log?(.debug, "[SynchronizedStore \(config.storeIdentifier)] changed keys: \(changedKeys.debugDescription)")
        
        var modified = [SynchronizationMetadata]()
        let allKeys = changedKeys ?? self.metadataStore.store.dictionaryRepresentation.keys.map { $0 }
        
        var newestModificationDate: Date = self.lastModificationDate
        for stringKey in allKeys {
            guard SynchronizationMetadataKey.isSynchronizationMetadataKey(stringKey) else {
                continue
            }
            
            let key = SynchronizationMetadataKey(id: stringKey)
            guard let metadata = try await self.metadataStore.load(with: key) else {
                fatalError("found missing key \(stringKey)")
            }
            guard metadata.databaseFilename != self.cloudDatabaseFilename else {
                continue
            }
            guard metadata.lastModificationDate > self.lastModificationDate else {
                continue
            }
            
            modified.append(metadata)
            newestModificationDate = max(newestModificationDate, metadata.lastModificationDate)
        }
        
        guard !modified.isEmpty else {
            return
        }
        
        try await self.synchronizeExternalChanges(changes: modified, newestModificationDate: newestModificationDate)
    }
    
    /// Update the local database representation based on external changes.
    func synchronizeExternalChanges(changes: [SynchronizationMetadata], newestModificationDate: Date) async throws {
        for change in changes {
            do {
                try await self.synchronizeChange(change: change)
            }
            catch {
                config.cryoConfig.log?(.error, "[SynchronizedStore \(config.storeIdentifier)] failed to synchronize change: \(error.localizedDescription)")
            }
        }
        
        config.cryoConfig.log?(.debug, "[SynchronizedStore \(config.storeIdentifier)] finished synchronising changes")
        
        // Persist changes
        try await self.publishChanges()
    }
    
    /// Handle a single external change.
    func synchronizeChange(change: SynchronizationMetadata) async throws {
        guard let cloudStore else {
            return
        }
        
        config.cryoConfig.log?(.debug, "[SynchronizedStore \(config.storeIdentifier)] synchronizing change \(change.databaseFilename)")
        
        let key = CryoNamedKey(id: change.databaseFilename, for: Data.self)
        guard let data = try await cloudStore.load(with: key) else {
            config.cryoConfig.log?(.error, "[SynchronizedStore \(config.storeIdentifier)] failed to load file")
            return
        }
        
        // Copy to a local file
        
        let tmpFile = DocumentAdaptor.sharedLocal.url.appendingPathComponent("\(UUID().uuidString).db")
        defer {
            do {
                try FileManager.default.removeItem(at: tmpFile)
            }
            catch {
                config.cryoConfig.log?(.error, "[SynchronizedStore \(config.storeIdentifier)] failed to delete tmp file: \(error.localizedDescription)")
            }
        }
        
        try data.write(to: tmpFile)
        
        let lastModificationDate = ISO8601DateFormatter().string(from: self.lastModificationDate)
        try await sqliteStore.withAttachedDatabase(databaseUrl: tmpFile) { databaseName in
            for modelType in config.managedModels {
                // Add new model instances
                try await sqliteStore
                    .query("""
INSERT OR IGNORE INTO \(modelType.tableName)
    SELECT * FROM \(databaseName).\(modelType.tableName);
""")
                    .execute()
                
                // Update modified instances
                try await sqliteStore
                    .query("""
INSERT OR REPLACE INTO \(modelType.tableName)
    SELECT * FROM \(databaseName).\(modelType.tableName) AS row
        WHERE row._cryo_modified > ?;
""")
                    .bind(lastModificationDate)
                    .execute()
            }
        }
    }
    
    /// The name of the database cloud document for this device.
    var cloudDatabaseFilename: String {
        "_csyncdb~\(config.storeIdentifier)-\(deviceIdentifier).db"
    }
    
    /// Publish changes to this store.
    func publishChanges() async throws {
        guard let cloudStore else {
            return
        }
        
        config.cryoConfig.log?(.debug, "[SynchronizedStore \(config.storeIdentifier)] publishing local changes")
        
        let date = Date.now
        let fileName = self.cloudDatabaseFilename
        
        // Copy file
        
        let data = try Data(contentsOf: config.databaseUrl)
        let key = CryoNamedKey(id: fileName, for: Data.self)
        
        try await cloudStore.persist(data, for: key)
        
        // Update metadata
        
        let metadataKey = SynchronizationMetadataKey(storeIdentifier: config.storeIdentifier,
                                                     deviceIdentifier: deviceIdentifier)
        let metadata = SynchronizationMetadata(lastModificationDate: date, databaseFilename: fileName,
                                               ubiquityIdentityToken: ubiquityIdentityToken)
        
        try await metadataStore.persist(metadata, for: metadataKey)
    }
}

// MARK: Keys

fileprivate struct SynchronizationMetadataKey: CryoKey {
    typealias Value = SynchronizationMetadata
    
    let id: String
    
    init(storeIdentifier: String, deviceIdentifier: String) {
        self.id = "_csyncmd~\(storeIdentifier)-\(deviceIdentifier)"
    }
    
    init(id: String) {
        assert(Self.isSynchronizationMetadataKey(id))
        self.id = id
    }
    
    static func isSynchronizationMetadataKey(_ key: String) -> Bool {
        key.starts(with: "_csyncmd~")
    }
}

// MARK: Conformances

extension SynchronizationMetadata: Codable { }
