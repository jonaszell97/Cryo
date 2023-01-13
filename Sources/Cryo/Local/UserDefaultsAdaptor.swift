
import Foundation

public struct UserDefaultsAdaptor {
    /// The UserDefaults instance.
    let defaults: UserDefaults
    
    /// Default initalizer.
    public init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
    }
}

extension UserDefaultsAdaptor: CryoAdaptor {
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws where Key.Value: CryoPersistable {
        guard let value else {
            defaults.removeObject(forKey: key.id)
            return
        }
        
        let persistableValue = try value.persistableValue
        switch persistableValue {
        case .integer(let value):
            defaults.set(value, forKey: key.id)
        case .double(let value):
            defaults.set(value, forKey: key.id)
        case .text(let value):
            defaults.set(value, forKey: key.id)
        case .date(let value):
            defaults.set(value, forKey: key.id)
        case .bool(let value):
            defaults.set(value, forKey: key.id)
        case .data(let value):
            defaults.set(value, forKey: key.id)
        }
    }
    
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value? where Key.Value: CryoPersistable {
        let cryoValue: CryoValue
        switch Key.Value.valueType {
        case .integer:
            guard defaults.object(forKey: key.id) != nil else { return nil }
            cryoValue = .integer(value: defaults.integer(forKey: key.id))
        case .double:
            guard defaults.object(forKey: key.id) != nil else { return nil }
            cryoValue = .double(value: defaults.double(forKey: key.id))
        case .text:
            guard let value = defaults.string(forKey: key.id) else { return nil }
            cryoValue = .text(value: value)
        case .date:
            guard let value = defaults.object(forKey: key.id) as? NSDate else { return nil }
            cryoValue = .date(value: value as Date)
        case .bool:
            guard defaults.object(forKey: key.id) != nil else { return nil }
            cryoValue = .bool(value: defaults.bool(forKey: key.id))
        case .data:
            guard let value = defaults.data(forKey: key.id) else { return nil }
            cryoValue = .data(value: value)
        }
        
        return Key.Value(from: cryoValue)
    }
    
    public func removeAll() async throws {
        let keys = defaults.dictionaryRepresentation().keys.map { $0 }
        for key in keys {
            defaults.removeObject(forKey: key)
        }
    }
}
