
import Foundation

internal protocol CryoPropertyWrapper {
    associatedtype Key: CryoKey
    
    var wrappedValue: Key.Value { get set }
    var adaptor: any CryoAdaptor { get }
    var key: Key { get }
}

extension CryoPropertyWrapper {
    /// Make several modifications to the wrapped value while only persisting it once at the end.
    public mutating func modify(_ modify: (inout Key.Value) -> Void) {
        var value = wrappedValue
        modify(&value)
        
        self.wrappedValue = value
    }
    
    /// Manually persist the value.
    public func persist() async throws {
        try await adaptor.persist(wrappedValue, for: key)
    }
}

@propertyWrapper public struct CryoPersisted<Value: Codable>: CryoPropertyWrapper {
    struct Key: CryoKey {
        let id: String
    }
    
    /// The ID of the persisted value.
    let id: String
    
    /// The adaptor to use for persistence.
    let adaptor: any CryoAdaptor
    
    /// The key for this instance.
    var key: Key { .init(id: id) }
    
    /// The wrapped value.
    public var wrappedValue: Value {
        didSet {
            let newValue = wrappedValue
            let adaptor = adaptor
            let id = id
            
            Task { try await adaptor.persist(newValue, for: Key(id: id)) }
        }
    }
    
    /// Memberwise initializer.
    public init(wrappedValue: Value, _ id: String, adaptor: any CryoAdaptor) {
        self.id = id
        self.adaptor = adaptor
        self.wrappedValue = (try? adaptor.loadSynchronously(with: Key(id: id))) ?? wrappedValue
    }
}

@propertyWrapper public struct CryoKeyValue<Value: Codable>: CryoPropertyWrapper {
    struct Key: CryoKey {
        let id: String
    }
    
    /// The ID of the persisted value.
    let id: String
    
    /// The adaptor.
    var adaptor: CryoAdaptor { UserDefaultsAdaptor.shared }
    
    /// The key for this instance.
    var key: Key { .init(id: id) }
    
    /// The wrapped value.
    public var wrappedValue: Value {
        didSet {
            let newValue = wrappedValue
            let adaptor = self.adaptor
            let id = id
            
            Task { try await adaptor.persist(newValue, for: Key(id: id)) }
        }
    }
    
    /// Memberwise initializer.
    public init(wrappedValue: Value, _ id: String) {
        self.id = id
        self.wrappedValue = (try? UserDefaultsAdaptor.shared.loadSynchronously(with: Key(id: id))) ?? wrappedValue
    }
}

@propertyWrapper public struct CryoUbiquitousKeyValue<Value: Codable>: CryoPropertyWrapper {
    struct Key: CryoKey {
        let id: String
    }
    
    /// The ID of the persisted value.
    let id: String
    
    /// The adaptor.
    var adaptor: CryoAdaptor { UbiquitousKeyValueStoreAdaptor.shared }
    
    /// The key for this instance.
    var key: Key { .init(id: id) }
    
    /// The wrapped value.
    public var wrappedValue: Value {
        didSet {
            let newValue = wrappedValue
            let adaptor = self.adaptor
            let id = id
            
            Task { try await adaptor.persist(newValue, for: Key(id: id)) }
        }
    }
    
    /// Memberwise initializer.
    public init(wrappedValue: Value, _ id: String) {
        self.id = id
        self.wrappedValue = wrappedValue
        self.wrappedValue = (try? UbiquitousKeyValueStoreAdaptor.shared.loadSynchronously(with: Key(id: id))) ?? wrappedValue
    }
}

@propertyWrapper public struct CryoLocalDocument<Value: Codable>: CryoPropertyWrapper {
    struct Key: CryoKey {
        let id: String
    }
    
    /// The ID of the persisted value.
    let id: String
    
    /// The adaptor.
    var adaptor: CryoAdaptor { DocumentAdaptor.sharedLocal }
    
    /// The key for this instance.
    var key: Key { .init(id: id) }
    
    /// The wrapped value.
    public var wrappedValue: Value {
        didSet {
            let newValue = wrappedValue
            let adaptor = self.adaptor
            let id = id
            
            Task { try await adaptor.persist(newValue, for: Key(id: id)) }
        }
    }
    
    /// Memberwise initializer.
    public init(wrappedValue: Value, _ id: String) {
        self.id = id
        self.wrappedValue = (try? DocumentAdaptor.sharedLocal.loadSynchronously(with: Key(id: id))) ?? wrappedValue
    }
}
