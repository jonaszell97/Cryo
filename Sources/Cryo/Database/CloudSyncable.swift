
#if canImport(UIKit)

import Foundation
import Toolbox
import UIKit

protocol CloudSyncableKey: CryoKey {
    /// Initialize from a device identifier.
    init(deviceIdentifier: String)
}

protocol CloudSyncableModel: CryoModel {
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

@MainActor protocol CloudSyncable: AnyObject, Codable {
    /// The local store type.
    associatedtype LocalStore: CryoAdaptor & CryoSynchronousAdaptor
    
    /// The remote store type.
    associatedtype RemoteStore: CloudKitAdaptor
    
    /// The model type.
    associatedtype ModelType: CloudSyncableModel
        where ModelType.Value == Self
    
    /// The local key type.
    associatedtype LocalKey: CloudSyncableKey
        where LocalKey.Value == Self
    
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
    static func localInstance(localStore: LocalStore, withIdentifier identifier: String) async -> Self?
    
    /// Load a remote instance with the given identifier.
    static func remoteInstance(remoteStore: RemoteStore, withIdentifier identifier: String) async -> Self?
    
    /// Load all remote instances.
    static func loadRemoteInstances(remoteStore: RemoteStore) async throws -> [Self]
    
    /// Remove an instance.
    func removeInstance(remoteStore: RemoteStore) async throws
    
    /// Save the current instance locally.
    func saveLocally(localStore: LocalStore) async
    
    /// Save the current instance remotely.
    func saveRemotely(remoteStore: RemoteStore) async
    
    /// Create a new instance with the given identifier.
    init(identifier: String)
    
    /// Consolidate the data from a local and remote instance.
    func consolidate(source: Self)
    
    /// Determine if an instance should be preferred over another.
    static func compare(lhs: Self, rhs: Self) -> Int
}

extension CloudSyncable {
    /// The name of this instance.
    var name: String { identifier }
    
    /// A number that represents the recency of this instance (higher numbers represent a more recent instance).
    var recency: Int { Int(lastModificationDate.timeIntervalSinceReferenceDate) }
    
    /// Determine if an instance should be preferred over another.
    static func compare(lhs: Self, rhs: Self) -> Int {
        Int(lhs.lastModificationDate.timeIntervalSinceReferenceDate) - Int(rhs.lastModificationDate.timeIntervalSinceReferenceDate)
    }
    
    /// Load a local instance with the given identifier.
    static func localInstance(localStore: LocalStore, withIdentifier identifier: String) async -> Self? {
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
    
    /// Load a remote instance with the given identifier.
    static func remoteInstance(remoteStore: RemoteStore, withIdentifier identifier: String) async -> Self? {
        Self.logger.log("[\(ModelType.self)] loading remote instance \(identifier)")
        defer {
            Self.logger.log("[\(ModelType.self)] finished loading remote instance \(identifier)")
        }
        
        do {
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
    static func loadRemoteInstances(remoteStore: RemoteStore) async throws -> [Self] {
        let modelInstances = try await remoteStore.select(from: ModelType.self).execute()
        logger.log("[\(ModelType.self)] Found \(modelInstances.count) total remote instances")
        
        var instances: [Self] = []
        for modelInstance in modelInstances {
            let deviceId = modelInstance.deviceIdentifier
            if deviceId == UIDevice.currentDeviceIdentifier {
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
    func saveLocally(localStore: LocalStore) async {
        do {
            Self.logger.log("[\(ModelType.self)] saving instance \(self.identifier) locally")
            
            let key = LocalKey(deviceIdentifier: identifier)
            try localStore.persistSynchronously(self, for: key)
        }
        catch {
            Self.logger.error("[\(ModelType.self)] error saving instance locally: \(error.localizedDescription)")
        }
    }
    
    /// Save the current instance remotely.
    func saveRemotely(remoteStore: RemoteStore) async {
        do {
            Self.logger.log("[\(ModelType.self)] saving instance \(self.identifier) remotely")
            
            let model = try ModelType(value: self)
            _ = try await remoteStore.insert(model, replace: true).execute()
        }
        catch {
            Self.logger.error("[\(ModelType.self)] error saving instance remotely: \(error.localizedDescription)")
        }
    }
    
    /// Remove this instance
    func removeInstance(remoteStore: RemoteStore) async throws {
        try await remoteStore.delete(id: ModelType.identifier(for: self.identifier), from: ModelType.self).execute()
    }
    
    /// Load the newest instance of a type.
    static func loadInstance(
        localStore: LocalStore,
        remoteStore: RemoteStore,
        withIdentifier identifier: String
    ) async -> Self {
        let localInstance = await Self.localInstance(
            localStore: localStore,
            withIdentifier: identifier
        ) ?? .init(identifier: identifier)
        logger.log("[\(ModelType.self)] Loaded local instance for device \(identifier)")
        
        do {
            let remoteInstances = try await Self.loadRemoteInstances(remoteStore: remoteStore)
            logger.log("[\(ModelType.self)] Found \(remoteInstances.count) other remote instances")
            
            var bestInstance = localInstance
            for remoteInstance in remoteInstances {
                if Self.compare(lhs: remoteInstance, rhs: bestInstance) > 0 {
                    logger.log("[\(ModelType.self)] Found better instance \(remoteInstance.name)")
                    bestInstance = remoteInstance
                }
            }
            
            if localInstance !== bestInstance {
                logger.log("[\(ModelType.self)] Copying data from best instance \(bestInstance.name)")
                localInstance.consolidate(source: bestInstance)
            }
            else {
                logger.log("[\(ModelType.self)] Using local instance \(localInstance.name)")
            }
            
            await localInstance.saveRemotely(remoteStore: remoteStore)
            try await Self.cleanupOldInstances(remoteStore: remoteStore, instances: remoteInstances)
        }
        catch {
            logger.error("[\(ModelType.self)] Failed to load cloud instances: \(error)")
        }
        
        await localInstance.saveLocally(localStore: localStore)
        return localInstance
    }
    
    /// Clean up old instances.
    static func cleanupOldInstances(remoteStore: RemoteStore, instances: [Self]) async throws {
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
                logger.log("[\(ModelType.self)] Deleting old instance \(instance.identifier) (recency \(instance.recency)).")
                try await instance.removeInstance(remoteStore: remoteStore)
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
