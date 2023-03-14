
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
    
    /// Shared instance using `UserDefaults.standard`.
    public static let shared: UserDefaultsAdaptor = UserDefaultsAdaptor(defaults: .standard)
    
    /// Create a user defaults adaptor.
    ///
    /// - Parameter defaults: The defaults instance to use.
    public init(defaults: UserDefaults) {
        self.defaults = defaults
    }
}

extension UserDefaultsAdaptor: CryoAdaptor, CryoSynchronousAdaptor {
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws {
        guard let value else {
            defaults.removeObject(forKey: key.id)
            return
        }
        
        switch value {
        case let v as String:
            defaults.set(v, forKey: key.id)
        case let v as URL:
            defaults.set(v, forKey: key.id)
        case let v as Double:
            defaults.set(v, forKey: key.id)
        case let v as Float:
            defaults.set(v, forKey: key.id)
        case let v as Bool:
            defaults.set(v, forKey: key.id)
        case let v as Int:
            defaults.set(v, forKey: key.id)
        case let v as Date:
            defaults.set(v.timeIntervalSinceReferenceDate, forKey: key.id)
        case let v as Data:
            defaults.set(v, forKey: key.id)
        default:
            defaults.set(try JSONEncoder().encode(value), forKey: key.id)
        }
    }
    
    public func loadSynchronously<Key: CryoKey>(with key: Key) throws -> Key.Value? {
        switch Key.Value.self {
        case is String.Type:
            guard defaults.object(forKey: key.id) != nil else { return nil }
            return defaults.string(forKey: key.id) as? Key.Value
        case is URL.Type:
            guard defaults.object(forKey: key.id) != nil else { return nil }
            return defaults.url(forKey: key.id) as? Key.Value
        case is Double.Type:
            guard defaults.object(forKey: key.id) != nil else { return nil }
            return defaults.double(forKey: key.id) as? Key.Value
        case is Float.Type:
            guard defaults.object(forKey: key.id) != nil else { return nil }
            return defaults.float(forKey: key.id) as? Key.Value
        case is Bool.Type:
            guard defaults.object(forKey: key.id) != nil else { return nil }
            return defaults.bool(forKey: key.id) as? Key.Value
        case is Int.Type:
            guard defaults.object(forKey: key.id) != nil else { return nil }
            return defaults.integer(forKey: key.id) as? Key.Value
        case is Date.Type:
            guard defaults.object(forKey: key.id) != nil else { return nil }
            return Date(timeIntervalSinceReferenceDate: defaults.double(forKey: key.id)) as? Key.Value
        case is Data.Type:
            guard defaults.object(forKey: key.id) != nil else { return nil }
            return defaults.data(forKey: key.id) as? Key.Value
        default:
            guard let data = defaults.data(forKey: key.id) else { return nil }
            return try JSONDecoder().decode(Key.Value.self, from: data)
        }
    }
    
    public func removeAll() async throws {
        let keys = defaults.dictionaryRepresentation().keys.map { $0 }
        for key in keys {
            defaults.removeObject(forKey: key)
        }
    }
}
