
#if canImport(UIKit)

import Foundation
import Toolbox
import UIKit

public protocol CloudSyncableKey: CryoKey {
    /// Initialize from a device identifier.
    init(deviceIdentifier: String)
    
    /// Identify whether a key string belongs to this key type.
    static func ownsInstanceWithKey(_ key: String) -> Bool
    
    /// Extract the device identifier from a key.
    static func deviceIdentifierFromKey(_ key: String) -> String
}

public extension CloudSyncableKey {
    init(id: String) {
        self.init(deviceIdentifier: id)
    }
}

public protocol CloudSyncableModel: CryoModel {
    /// The wrapped value type.
    associatedtype Value
    
    /// Initialize from an instance of the value type.
    init(value: Value) throws
    
    /// The device identifier.
    var deviceIdentifier: String { get }
    
    /// Create an instance of the wrapped value type.
    func createInstance() throws -> Value
    
    /// Get a model identfiier for a device identifier.
    static func identifier(for deviceIdentifier: String) -> String
}

@MainActor public protocol CloudSyncable: AnyObject, Codable {
    /// The model type.
    associatedtype ModelType: CloudSyncableModel
        where ModelType.Value == Self
    
    /// The local key type.
    associatedtype LocalKey: CloudSyncableKey
        where LocalKey.Value == Self
    
    /// The local store type.
    associatedtype LocalStore: CryoAdaptor & CryoSynchronousAdaptor
    
    /// The remote store type.
    associatedtype RemoteStore: CloudKitAdaptor
    
    /// Get the local store instance.
    static var localStore: LocalStore { get }
    
    /// Get the remote store instance.
    static var remoteStore: RemoteStore? { get }
    
    /// The identifier of this instance.
    var identifier: String { get }
    
    /// The last modification date of this instance.
    var lastModificationDate: Date { get }
    
    /// The name of this instance.
    var name: String { get }
    
    /// A number that represents the recency of this instance (higher numbers represent a more recent instance).
    var recency: Int { get }
    
    /// The logger instance to use.
    static var logger: Logger { get }
    
    /// Load a local instance with the given identifier.
    static func localInstance(withIdentifier identifier: String) async -> Self?
    
    /// Load a remote instance with the given identifier.
    static func remoteInstance(withIdentifier identifier: String) async -> Self?
    
    /// Load all local instances.
    static func loadLocalInstances() async throws -> [Self]?
    
    /// Load all remote instances.
    static func loadRemoteInstances(includeSelf: Bool) async throws -> [Self]
    
    /// Remove an instance.
    func removeInstance() async throws
    
    /// Save the current instance locally.
    func saveLocally() async
    
    /// Save the current instance remotely.
    func saveRemotely() async
    
    /// Create a new instance with the given identifier.
    init(identifier: String)
    
    /// Consolidate the data from a local and remote instance.
    func consolidate(source: Self)

    /// Merge the best available remote instance into this live local instance.
    @discardableResult
    func mergeWithRemoteInstances() async -> Bool
    
    /// Determine if an instance should be preferred over another.
    static func compare(lhs: Self, rhs: Self) -> Int
}

public extension CloudSyncable {
    /// The name of this instance.
    var name: String { identifier }
    
    /// A number that represents the recency of this instance (higher numbers represent a more recent instance).
    var recency: Int { Int(lastModificationDate.timeIntervalSinceReferenceDate) }
    
    /// Determine if an instance should be preferred over another.
    static func compare(lhs: Self, rhs: Self) -> Int {
        Int(lhs.lastModificationDate.timeIntervalSinceReferenceDate) - Int(rhs.lastModificationDate.timeIntervalSinceReferenceDate)
    }
    
    /// Load a local instance with the given identifier.
    static func localInstance(withIdentifier identifier: String) async -> Self? {
        Self.logger.log("[\(ModelType.self)] loading local instance \(identifier)")
        defer {
            Self.logger.log("[\(ModelType.self)] finished loading local instance \(identifier)")
        }
        
        do {
            let key = LocalKey(deviceIdentifier: identifier)
            return try await localStore.load(with: key)
        }
        catch {
            logger.error("[\(ModelType.self)] failed to decode local instance \(identifier)")
            return nil
        }
    }
    
    /// Load all available local instances.
    static func loadLocalInstances() -> [Self]? {
        Self.logger.log("[\(ModelType.self)] loading all local instances")
        defer {
            Self.logger.log("[\(ModelType.self)] finished loading all local instances")
        }
        
        do {
            guard let keys = try localStore.listInstanceKeysSynchronously() else {
                return nil
            }
            
            var result: [Self] = []
            for key in keys {
                guard LocalKey.ownsInstanceWithKey(key) else {
                    continue
                }
                
                let deviceIdentifier = LocalKey.deviceIdentifierFromKey(key)
                result.append(ifNotNil: try localStore.loadSynchronously(
                    with: LocalKey(deviceIdentifier: deviceIdentifier))
                )
            }
            
            return result
        }
        catch {
            logger.error("[\(ModelType.self)] failed to load all local instances: \(error)")
            return []
        }
    }
    
    /// Load a remote instance with the given identifier.
    static func remoteInstance(withIdentifier identifier: String) async -> Self? {
        Self.logger.log("[\(ModelType.self)] loading remote instance \(identifier)")
        defer {
            Self.logger.log("[\(ModelType.self)] finished loading remote instance \(identifier)")
        }
        
        do {
            guard let remoteStore else {
                logger.error("[\(ModelType.self)] Remote store not initialized")
                return nil
            }
            
            let id = ModelType.identifier(for: identifier)
            let results = try await remoteStore.select(id: id, from: ModelType.self).execute()
            
            guard let model = results.first else {
                logger.warning("[\(ModelType.self)] no remote instance found for identifier \(identifier)")
                return nil
            }
            
            return try model.createInstance()
        }
        catch {
            logger.error("[\(ModelType.self)] failed to decode remote instance \(identifier)")
            return nil
        }
    }
    
    /// Load all remote instances.
    static func loadRemoteInstances(includeSelf: Bool = false) async throws -> [Self] {
        guard let remoteStore else {
            logger.error("[\(ModelType.self)] Remote store not initialized")
            return []
        }
        
        let modelInstances = try await remoteStore.select(from: ModelType.self).execute()
        logger.log("[\(ModelType.self)] Found \(modelInstances.count) total remote instances")
        
        var instances: [Self] = []
        for modelInstance in modelInstances {
            let deviceId = modelInstance.deviceIdentifier
            if !includeSelf && deviceId == UIDevice.currentDeviceIdentifier {
                continue
            }
            
            guard let instance = try? modelInstance.createInstance() else {
                logger.error("[\(ModelType.self)] failed to decode remote instance \(modelInstance.deviceIdentifier)")
                continue
            }
            
            instances.append(instance)
        }
        
        return instances
    }
    
    /// Save the current instance locally.
    func saveLocally() async {
        do {
            Self.logger.log("[\(ModelType.self)] saving instance \(self.identifier) locally")
            
            let key = LocalKey(deviceIdentifier: identifier)
            try Self.localStore.persistSynchronously(self, for: key)
        }
        catch {
            Self.logger.error("[\(ModelType.self)] error saving instance locally: \(error.localizedDescription)")
        }
    }
    
    /// Save the current instance remotely.
    func saveRemotely() async {
        do {
            guard let remoteStore = Self.remoteStore else {
                Self.logger.error("[\(ModelType.self)] Remote store not initialized")
                return
            }
            
            Self.logger.log("[\(ModelType.self)] saving instance \(self.identifier) remotely")
            
            let model = try ModelType(value: self)
            _ = try await remoteStore.insert(model, replace: true).execute()
        }
        catch {
            Self.logger.error("[\(ModelType.self)] error saving instance remotely: \(error.localizedDescription)")
        }
    }
    
    /// Remove this instance
    func removeInstance() async throws {
        guard let remoteStore = Self.remoteStore else {
            Self.logger.error("[\(ModelType.self)] Remote store not initialized")
            return
        }
        
        try await remoteStore.delete(
            id: ModelType.identifier(for: self.identifier), from: ModelType.self
        ).execute()
    }
    
    /// Load the newest instance of a type.
    static func loadInstance(withIdentifier identifier: String, loadRemoteInstances: Bool = true) async -> Self {
        let localInstance = await Self.localInstance(withIdentifier: identifier) ?? .init(identifier: identifier)
        logger.log("[\(ModelType.self)] Loaded local instance for device \(identifier) with recency \(localInstance.recency)")
        
        guard loadRemoteInstances else {
            logger.log("[\(ModelType.self)] Not loading remote instances")
            return localInstance
        }
        
        _ = await localInstance.mergeWithRemoteInstances()
        return localInstance
    }

    /// Merge the best available remote instance into this live local instance.
    ///
    /// Keeping the receiver alive is important for local-first startup: views and
    /// callbacks continue to reference the same object when CloudKit becomes
    /// available after launch.
    @discardableResult
    func mergeWithRemoteInstances() async -> Bool {
        do {
            let remoteInstances = try await Self.loadRemoteInstances()
            Self.logger.log("[\(ModelType.self)] Found \(remoteInstances.count) other remote instances")

            var bestInstance = self
            for remoteInstance in remoteInstances {
                if Self.compare(lhs: remoteInstance, rhs: bestInstance) > 0 {
                    Self.logger.log("[\(ModelType.self)] Found better instance \(remoteInstance.name) with recency \(remoteInstance.recency) (compared with \(bestInstance.name) \(bestInstance.recency)")
                    bestInstance = remoteInstance
                }
            }

            if self !== bestInstance {
                Self.logger.log("[\(ModelType.self)] Copying data from best instance \(bestInstance.name) with recency \(bestInstance.recency)")
                consolidate(source: bestInstance)
            }
            else {
                Self.logger.log("[\(ModelType.self)] Using local instance \(name) with recency \(recency)")
            }

            try await Self.cleanupOldInstances(instances: remoteInstances)
            await saveLocally()
            return true
        }
        catch {
            Self.logger.error("[\(ModelType.self)] Failed to merge cloud instances: \(error)")
            return false
        }
    }
    
    /// Clean up old instances.
    static func cleanupOldInstances(instances: [Self]) async throws {
        logger.log("[\(ModelType.self)] cleaning up \(instances.count) old instances")
        
        var highestRecencyByDeviceId: [String: (Int, Self)] = [:]
        for instance in instances {
            if let (highestRecency, _) = highestRecencyByDeviceId[instance.identifier] {
                if instance.recency > highestRecency {
                    highestRecencyByDeviceId[instance.identifier] = (instance.recency, instance)
                }
            }
            else {
                highestRecencyByDeviceId[instance.identifier] = (instance.recency, instance)
            }
        }
        
        for instance in instances {
            guard let (_, highestRecencyInstance) = highestRecencyByDeviceId[instance.identifier] else {
                continue
            }
            
            if instance.identifier != highestRecencyInstance.identifier {
                logger.log("[\(ModelType.self)] Deleting old instance \(instance.identifier) from device \(instance.name) (recency \(instance.recency)).")
                try await instance.removeInstance()
            }
        }
        
        logger.log("[\(ModelType.self)] finished cleaning up old instances for type")
    }
}


fileprivate extension UIDevice {
    static var currentDeviceIdentifier: String {
        current.identifierForVendor?.uuidString ?? ""
    }
}

#endif
