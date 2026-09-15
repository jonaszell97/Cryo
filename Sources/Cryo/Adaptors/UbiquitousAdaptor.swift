
import Foundation

/// An implementation of ``CryoAdaptor`` using `NSUbiquitousKeyValueStore` as a storage backend.
///
/// This adaptor can natively store values of type `Int`, `Bool`, `Double`, `Float`, `String`, `Date`, `URL`, and `Data`.
/// All other values will be encoded using a `JSONEncoder` and stored as `Data`.
///
/// ```swift
/// let adaptor = UbiquitousKeyValueStoreAdaptor.shared
/// try await adaptor.persist(3, CryoNamedKey(id: "intValue", for: Int.self))
/// try await adaptor.persist("Hi there", CryoNamedKey(id: "stringValue", for: String.self))
/// try await adaptor.persist(Date.now, CryoNamedKey(id: "dateValue", for: Date.self))
/// ```
public final class UbiquitousKeyValueStoreAdaptor {
    /// The UserDefaults instance.
    let store: NSUbiquitousKeyValueStore
    public let keyPrefix: String?
    public let config: CryoConfig
    
    /// List of active Change observers.
    fileprivate var observers: [UbiquitousKeyValueStoreObserver] = []
    private let observersLock = NSLock()
    
    /// Shared instance using the `NSUbiquitousKeyValueStore.default`.
    public static let shared: UbiquitousKeyValueStoreAdaptor = UbiquitousKeyValueStoreAdaptor(store: .default)
    
    /// Create a ubiquitous key value adaptor.
    ///
    /// - Parameter store: The store instance to use.
    public init(store: NSUbiquitousKeyValueStore = .default, keyPrefix: String? = nil,
                config: CryoConfig = .init()) {
        self.store = store
        self.keyPrefix = keyPrefix
        self.config = config
        if !store.synchronize() {
            config.log?(.error, "[UbiquitousKeyValueStoreAdaptor] synchronize returned false")
        }
    }

    private func storageKey(_ id: String) -> String { (keyPrefix ?? "") + id }
}

extension UbiquitousKeyValueStoreAdaptor: CryoAdaptor, CryoSynchronousAdaptor {
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws {
        try self.persistSynchronously(value, for: key)
    }
    
    public func persistSynchronously<Key: CryoKey>(_ value: Key.Value?, for key: Key) throws {
        guard let value else {
            store.removeObject(forKey: storageKey(key.id))
            return
        }

        let id = storageKey(key.id)
        switch Key.Value.self {
        case is String.Type:
            store.set((value as! String), forKey: id)
        case is Double.Type:
            store.set(value as! Double, forKey: id)
        case is Float.Type:
            store.set(Double(value as! Float), forKey: id)
        case is Bool.Type:
            store.set(value as! Bool, forKey: id)
        case is Int.Type:
            store.set(Int64(value as! Int), forKey: id)
        case is Date.Type:
            store.set((value as! Date).timeIntervalSinceReferenceDate, forKey: id)
        case is Data.Type:
            store.set((value as! Data), forKey: id)
        default:
            store.set(try JSONEncoder().encode(value), forKey: id)
        }
    }
    
    public func loadSynchronously<Key: CryoKey>(with key: Key) throws -> Key.Value? {
        let id = storageKey(key.id)
        switch Key.Value.self {
        case is String.Type:
            guard store.object(forKey: id) != nil else { return nil }
            return store.string(forKey: id) as? Key.Value
        case is Double.Type:
            guard store.object(forKey: id) != nil else { return nil }
            return store.double(forKey: id) as? Key.Value
        case is Float.Type:
            guard store.object(forKey: id) != nil else { return nil }
            return Float(store.double(forKey: id)) as? Key.Value
        case is Bool.Type:
            guard store.object(forKey: id) != nil else { return nil }
            return store.bool(forKey: id) as? Key.Value
        case is Int.Type:
            guard store.object(forKey: id) != nil else { return nil }
            return Int(store.longLong(forKey: id)) as? Key.Value
        case is Date.Type:
            guard store.object(forKey: id) != nil else { return nil }
            return Date(timeIntervalSinceReferenceDate: store.double(forKey: id)) as? Key.Value
        case is Data.Type:
            guard store.object(forKey: id) != nil else { return nil }
            return store.data(forKey: id) as? Key.Value
        default:
            guard let data = store.data(forKey: id) else { return nil }
            return try JSONDecoder().decode(Key.Value.self, from: data)
        }
    }
    
    public func synchronize() {
        if !store.synchronize() {
            config.log?(.error, "[UbiquitousKeyValueStoreAdaptor] synchronize returned false")
        }
    }
    
    public func removeAll() async throws {
        try self.removeAllSynchronously()
    }
    
    public func removeAllSynchronously() throws {
        let keys = store.dictionaryRepresentation.keys.filter { keyPrefix == nil || $0.hasPrefix(keyPrefix!) }
        for key in keys {
            store.removeObject(forKey: key)
        }
    }
}

public struct UbiquitousKeyValueStoreChangeData {
    enum ChangeReason: String {
        case unknown, dataChanged, initalSync, quotaViolation, accountChange
    }
    
    /// The change reason.
    var reason: ChangeReason = .unknown
    
    /// The changed keys.
    var changedKeys: [String]? = nil
}

fileprivate final class UbiquitousKeyValueStoreObserver: NSObject {
    /// The user callback.
    let callback: (UbiquitousKeyValueStoreChangeData) -> Void
    
    /// Create an observer.
    init(callback: @escaping (UbiquitousKeyValueStoreChangeData) -> Void) {
        self.callback = callback
    }
    
    /// Install the observer.
    func register(store: NSUbiquitousKeyValueStore) {
        NotificationCenter.default.addObserver(self,
                                               selector: #selector(ubiquitousKeyValueStoreDidChange(_:)),
                                               name: NSUbiquitousKeyValueStore.didChangeExternallyNotification,
                                               object: store)
    }
    
    /// Unregister the observer.
    func unregister() {
        NotificationCenter.default.removeObserver(self)
    }

    deinit { unregister() }
    
    @objc private func ubiquitousKeyValueStoreDidChange(_ notification: Notification) {
        var data = UbiquitousKeyValueStoreChangeData()
        if let userInfo = notification.userInfo {
            if let reasonForChange = userInfo[NSUbiquitousKeyValueStoreChangeReasonKey] as? Int {
                switch reasonForChange {
                case NSUbiquitousKeyValueStoreServerChange:
                    data.reason = .dataChanged
                case NSUbiquitousKeyValueStoreInitialSyncChange:
                    data.reason = .initalSync
                case NSUbiquitousKeyValueStoreQuotaViolationChange:
                    data.reason = .quotaViolation
                case NSUbiquitousKeyValueStoreAccountChange:
                    data.reason = .accountChange
                default:
                    data.reason = .unknown
                }
            }
            
            data.changedKeys = userInfo[NSUbiquitousKeyValueStoreChangedKeysKey] as? [String]
        }
        
        callback(data)
    }
}

extension UbiquitousKeyValueStoreAdaptor: CryoObservableAdaptor {
    /// Install a listener for external changes.
    public func observeChanges(_ callback: @escaping (UbiquitousKeyValueStoreChangeData) -> Void) -> ObjectIdentifier {
        let observer = UbiquitousKeyValueStoreObserver(callback: callback)
        observer.register(store: store)

        observersLock.lock()
        self.observers.append(observer)
        observersLock.unlock()
        return ObjectIdentifier(observer)
    }
    
    /// Remove a change observer.
    public func removeObserver(withId id: ObjectIdentifier) {
        observersLock.lock()
        guard let observerIndex = (self.observers.firstIndex { id == ObjectIdentifier($0) }) else {
            observersLock.unlock()
            return
        }
        let observer = self.observers.remove(at: observerIndex)
        observersLock.unlock()
        observer.unregister()
    }
}
