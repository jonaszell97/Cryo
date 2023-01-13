
import Foundation

public struct UbiquitousKeyValueStoreAdaptor {
    /// The UserDefaults instance.
    let store: NSUbiquitousKeyValueStore
    
    /// Default initalizer.
    public init(store: NSUbiquitousKeyValueStore = .default) {
        self.store = store
        store.synchronize()
    }
}

extension UbiquitousKeyValueStoreAdaptor: CryoAdaptor {
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws where Key.Value: CryoPersistable {
        guard let value else {
            store.removeObject(forKey: key.id)
            return
        }
        
        let persistableValue = try value.persistableValue
        switch persistableValue {
        case .integer(let value):
            store.set(value, forKey: key.id)
        case .double(let value):
            store.set(value, forKey: key.id)
        case .text(let value):
            store.set(value, forKey: key.id)
        case .date(let value):
            store.set(value, forKey: key.id)
        case .bool(let value):
            store.set(value, forKey: key.id)
        case .data(let value):
            store.set(value, forKey: key.id)
        }
    }
    
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value? where Key.Value: CryoPersistable {
        let cryoValue: CryoValue
        switch Key.Value.valueType {
        case .integer:
            guard store.object(forKey: key.id) != nil else { return nil }
            cryoValue = .integer(value: Int(store.longLong(forKey: key.id)))
        case .double:
            guard store.object(forKey: key.id) != nil else { return nil }
            cryoValue = .double(value: store.double(forKey: key.id))
        case .text:
            guard let value = store.string(forKey: key.id) else { return nil }
            cryoValue = .text(value: value)
        case .date:
            guard let value = store.object(forKey: key.id) as? NSDate else { return nil }
            cryoValue = .date(value: value as Date)
        case .bool:
            guard store.object(forKey: key.id) != nil else { return nil }
            cryoValue = .bool(value: store.bool(forKey: key.id))
        case .data:
            guard let value = store.data(forKey: key.id) else { return nil }
            cryoValue = .data(value: value)
        }
        
        return Key.Value(from: cryoValue)
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
