
import CloudKit
import Foundation

public struct MirroredDatabaseStoreConfig {
    /// A unique identifier for this store.
    let identifier: String
    
    /// The cryo config.
    let config: CryoConfig
    
    /// Create a config for a ``MirroredDatabaseStore``.
    public init(identifier: String, config: CryoConfig) {
        self.identifier = identifier
        self.config = config
    }
}

public final class MirroredDatabaseStore<MainAdaptor: CryoDatabaseAdaptor> {
    /// The configuration for this adaptor.
    let config: MirroredDatabaseStoreConfig
    
    /// The CloudKit adaptor.
    let mainAdaptor: MainAdaptor
    
    /// The mirror adaptor.
    let mirrorAdaptor: SQLiteAdaptor
    
    /// The adaptor used to store operations.
    let operationsAdaptor: DocumentAdaptor
    
    /// Create a locally mirrored database adaptor.
    public init(config: MirroredDatabaseStoreConfig, mainAdaptor: MainAdaptor) throws {
        self.config = config
        self.mainAdaptor = mainAdaptor
        self.operationsAdaptor = DocumentAdaptor.local(subdirectory: "_crlst_\(config.identifier)")
        self.mirrorAdaptor = try .init(databaseUrl: operationsAdaptor.url.appendingPathComponent("mirror.db"), config: config.config)
    }
}

extension MirroredDatabaseStore {
    /// Flush the local stores.
    func executeQueuedOperations() async throws {
        do {
            try await mainAdaptor.ensureAvailability()
        }
        catch {
            return
        }
        
        config.config.log?(.debug, "[MirroredDatabaseStore][\(config.identifier)] flushing local stores")
        
        let urls = try operationsAdaptor.fileManager.contentsOfDirectory(at: operationsAdaptor.url, includingPropertiesForKeys: nil)
            .sorted { $0.lastPathComponent < $1.lastPathComponent}
        
        for url in urls {
            let id = url.lastPathComponent
            
            let key = CryoNamedKey(id: id, for: DatabaseOperation.self)
            guard let operation = try await operationsAdaptor.load(with: key) else {
                continue
            }
            
            try await self.dequeue(operation: operation, id: id)
        }
    }
    
    /// Execute  a remote operation.
    func execute(operation: DatabaseOperation, enqueueIfFailed: Bool = true) async throws {
        do {
            try await mirrorAdaptor.execute(operation: operation)
            try await mainAdaptor.execute(operation: operation)
            
            return
        }
        catch let e as CryoError {
            guard case .backendNotAvailable = e else {
                throw e
            }
        }
        
        guard enqueueIfFailed else {
            return
        }
        
        // Queue the operation to be executed later
        try await self.enqueue(operation: operation)
    }
    
    /// Locally queue an operation to be executed once CloudKit is available.
    func enqueue(operation: DatabaseOperation) async throws {
        let id = ISO8601DateFormatter().string(from: operation.date) + UUID().uuidString
        let key = CryoNamedKey(id: id, for: DatabaseOperation.self)
        
        try await operationsAdaptor.persist(operation, for: key)
    }
    
    /// Try to execute an operation and dequeue it if successful.
    func dequeue(operation: DatabaseOperation, id: String) async throws {
        do {
            try await mainAdaptor.execute(operation: operation)
        }
        catch {
            return
        }
        
        let key = CryoNamedKey(id: id, for: DatabaseOperation.self)
        try await operationsAdaptor.remove(with: key)
    }
}

extension MirroredDatabaseStore: CryoIndexingAdaptor {
   public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws
        where Key.Value: CryoModel
    {
        guard let value else {
            try await self.remove(with: key)
            return
        }
        
        let operation = DatabaseOperation.insert(tableName: Key.Value.tableName,
                                                 id: key.id,
                                                 data: try value.codableData)
        
        try await self.execute(operation: operation)
    }
    
    public func remove<Key: CryoKey>(with key: Key) async throws
        where Key.Value: CryoModel
    {
        let operation = DatabaseOperation.delete(tableName: Key.Value.tableName, id: key.id)
        try await self.execute(operation: operation)
    }
    
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value?
        where Key.Value: CryoModel
    {
        do {
            try await mainAdaptor.ensureAvailability()
        }
        catch {
            return try await mirrorAdaptor.load(with: key)
        }
        
        return try await mainAdaptor.load(with: key)
    }
    
    public func loadAll<Record>(of type: Record.Type) async throws -> [Record]?
        where Record: CryoModel
    {
        do {
            try await mainAdaptor.ensureAvailability()
        }
        catch {
            return try await mirrorAdaptor.loadAll(of: type)
        }
        
        return try await mainAdaptor.loadAll(of: type)
    }
    
    public func removeAll<Record>(of type: Record.Type) async throws
        where Record: CryoModel
    {
        let operation = DatabaseOperation.delete(tableName: Record.tableName)
        try await self.execute(operation: operation)
    }
    
    public func removeAll() async throws {
        let operation = DatabaseOperation.deleteAll()
        try await self.execute(operation: operation)
    }
}
