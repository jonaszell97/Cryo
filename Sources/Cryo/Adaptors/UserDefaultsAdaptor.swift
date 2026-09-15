
import Foundation

/// An implementation of ``CryoAdaptor`` using `UserDefaults` as a storage backend.
///
/// This adaptor can natively store values of type `Int`, `Bool`, `Double`, `Float`, `String`, `Date`, `URL`, and `Data`.
/// All other values will be encoded using a `JSONEncoder` and stored as `Data`.
///
/// ```swift
/// let adaptor = UserDefaultsAdaptor.shared
/// try await adaptor.persist(3, CryoNamedKey(id: "intValue", for: Int.self))
/// try await adaptor.persist("Hi there", CryoNamedKey(id: "stringValue", for: String.self))
/// try await adaptor.persist(Date.now, CryoNamedKey(id: "dateValue", for: Date.self))
/// ```
public struct UserDefaultsAdaptor {
    /// The UserDefaults instance.
    let defaults: UserDefaults

    /// Optional namespace used for keys and for scoped `removeAll` operations.
    public let keyPrefix: String?
    
    /// Shared instance using `UserDefaults.standard`.
    public static let shared: UserDefaultsAdaptor = UserDefaultsAdaptor(defaults: .standard)
    
    /// Create a user defaults adaptor.
    ///
    /// - Parameter defaults: The defaults instance to use.
    public init(defaults: UserDefaults, keyPrefix: String? = nil) {
        self.defaults = defaults
        self.keyPrefix = keyPrefix
    }

    private func storageKey(_ id: String) -> String { (keyPrefix ?? "") + id }
}

extension UserDefaultsAdaptor: CryoAdaptor, CryoSynchronousAdaptor {
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws {
        try self.persistSynchronously(value, for: key)
    }
    
    public func persistSynchronously<Key: CryoKey>(_ value: Key.Value?, for key: Key) throws {
        guard let value else {
            defaults.removeObject(forKey: storageKey(key.id))
            return
        }

        let id = storageKey(key.id)
        switch Key.Value.self {
        case is String.Type:
            defaults.set(value as! String, forKey: id)
        case is URL.Type:
            defaults.set((value as! URL), forKey: id)
        case is Double.Type:
            defaults.set(value as! Double, forKey: id)
        case is Float.Type:
            defaults.set(value as! Float, forKey: id)
        case is Bool.Type:
            defaults.set(value as! Bool, forKey: id)
        case is Int.Type:
            defaults.set(value as! Int, forKey: id)
        case is Date.Type:
            defaults.set((value as! Date).timeIntervalSinceReferenceDate, forKey: id)
        case is Data.Type:
            defaults.set(value as! Data, forKey: id)
        default:
            defaults.set(try JSONEncoder().encode(value), forKey: id)
        }
    }
    
    public func loadSynchronously<Key: CryoKey>(with key: Key) throws -> Key.Value? {
        let id = storageKey(key.id)
        switch Key.Value.self {
        case is String.Type:
            guard defaults.object(forKey: id) != nil else { return nil }
            return defaults.string(forKey: id) as? Key.Value
        case is URL.Type:
            guard defaults.object(forKey: id) != nil else { return nil }
            return defaults.url(forKey: id) as? Key.Value
        case is Double.Type:
            guard defaults.object(forKey: id) != nil else { return nil }
            return defaults.double(forKey: id) as? Key.Value
        case is Float.Type:
            guard defaults.object(forKey: id) != nil else { return nil }
            return defaults.float(forKey: id) as? Key.Value
        case is Bool.Type:
            guard defaults.object(forKey: id) != nil else { return nil }
            return defaults.bool(forKey: id) as? Key.Value
        case is Int.Type:
            guard defaults.object(forKey: id) != nil else { return nil }
            return defaults.integer(forKey: id) as? Key.Value
        case is Date.Type:
            guard defaults.object(forKey: id) != nil else { return nil }
            return Date(timeIntervalSinceReferenceDate: defaults.double(forKey: id)) as? Key.Value
        case is Data.Type:
            guard defaults.object(forKey: id) != nil else { return nil }
            return defaults.data(forKey: id) as? Key.Value
        default:
            guard let data = defaults.data(forKey: id) else { return nil }
            return try JSONDecoder().decode(Key.Value.self, from: data)
        }
    }
    
    public func removeAll() async throws {
        try self.removeAllSynchronously()
    }
    
    public func removeAllSynchronously() throws {
        let keys = defaults.dictionaryRepresentation().keys.filter { keyPrefix == nil || $0.hasPrefix(keyPrefix!) }
        for key in keys {
            defaults.removeObject(forKey: key)
        }
    }
}
