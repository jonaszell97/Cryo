
import Foundation

public struct UbiquitousKeyValueStoreAdaptor {
    /// The UserDefaults instance.
    let store: NSUbiquitousKeyValueStore
    
    /// Shared instance using the default NSUbiquitousKeyValueStore.
    public static let shared: UbiquitousKeyValueStoreAdaptor = UbiquitousKeyValueStoreAdaptor(store: .default)
    
    /// Default initalizer.
    public init(store: NSUbiquitousKeyValueStore = .default) {
        self.store = store
        store.synchronize()
    }
}

extension UbiquitousKeyValueStoreAdaptor: CryoAdaptor {
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws {
        guard let value else {
            store.removeObject(forKey: key.id)
            return
        }
        
        switch value {
        case let v as String:
            store.set(v, forKey: key.id)
        case let v as Double:
            store.set(v, forKey: key.id)
        case let v as Float:
            store.set(Double(v), forKey: key.id)
        case let v as Bool:
            store.set(v, forKey: key.id)
        case let v as Int:
            store.set(Int64(v), forKey: key.id)
        case let v as Date:
            store.set(v.timeIntervalSinceReferenceDate, forKey: key.id)
        case let v as Data:
            store.set(v, forKey: key.id)
        default:
            store.set(try JSONEncoder().encode(value), forKey: key.id)
        }
    }
    
    public func loadSynchronously<Key: CryoKey>(with key: Key) throws -> Key.Value? {
        switch Key.Value.self {
        case is String.Type:
            guard store.object(forKey: key.id) != nil else { return nil }
            return store.string(forKey: key.id) as? Key.Value
        case is Double.Type:
            guard store.object(forKey: key.id) != nil else { return nil }
            return store.double(forKey: key.id) as? Key.Value
        case is Float.Type:
            guard store.object(forKey: key.id) != nil else { return nil }
            return Float(store.double(forKey: key.id)) as? Key.Value
        case is Bool.Type:
            guard store.object(forKey: key.id) != nil else { return nil }
            return store.bool(forKey: key.id) as? Key.Value
        case is Int.Type:
            guard store.object(forKey: key.id) != nil else { return nil }
            return Int(store.longLong(forKey: key.id)) as? Key.Value
        case is Date.Type:
            guard store.object(forKey: key.id) != nil else { return nil }
            return Date(timeIntervalSinceReferenceDate: store.double(forKey: key.id)) as? Key.Value
        case is Data.Type:
            guard store.object(forKey: key.id) != nil else { return nil }
            return store.data(forKey: key.id) as? Key.Value
        default:
            guard let data = store.data(forKey: key.id) else { return nil }
            return try JSONDecoder().decode(Key.Value.self, from: data)
        }
    }
    
    public func synchronize() {
        store.synchronize()
    }
    
    public func removeAll() async throws {
        let keys = store.dictionaryRepresentation.keys.map { $0 }
        for key in keys {
            store.removeObject(forKey: key)
        }
    }
}
