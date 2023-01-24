
import Foundation

@propertyWrapper public struct CryoPersisted<Value: Codable> {
    struct Key: CryoKey {
        let id: String
    }
    
    /// The ID of the persisted value.
    let id: String
    
    /// The adaptor to use for persistence.
    let adaptor: any CryoAdaptor
    
    /// The key for this instance.
    var key: Key { .init(id: id) }
    
    /// Whether to automatically save after each modification.
    let saveOnWrite: Bool
    
    /// The wrapped value.
    public var wrappedValue: Value {
        didSet {
            guard saveOnWrite else { return }
            let newValue = wrappedValue
            let adaptor = adaptor
            let id = id
            
            Task { try await adaptor.persist(newValue, for: Key(id: id)) }
        }
    }
    
    /// Memberwise initializer.
    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true, adaptor: any CryoAdaptor) {
        self.id = id
        self.adaptor = adaptor
        self.saveOnWrite = saveOnWrite
        self.wrappedValue = (try? adaptor.loadSynchronously(with: Key(id: id))) ?? wrappedValue
    }
    
    /// Make several modifications to the wrapped value while only persisting it once at the end.
    public mutating func modify(_ modify: (inout Value) -> Void) {
        var value = wrappedValue
        modify(&value)
        
        self.wrappedValue = value
    }
    
    /// Manually persist the value.
    public func persist() async throws {
        try await adaptor.persist(wrappedValue, for: key)
    }
}

@propertyWrapper public struct CryoKeyValue<Value: Codable> {
    struct Key: CryoKey {
        let id: String
    }
    
    /// The ID of the persisted value.
    let id: String
    
    /// The adaptor.
    var adaptor: CryoAdaptor { UserDefaultsAdaptor.shared }
    
    /// The key for this instance.
    var key: Key { .init(id: id) }
    
    /// Whether to automatically save after each modification.
    let saveOnWrite: Bool
    
    /// The wrapped value.
    public var wrappedValue: Value {
        didSet {
            guard saveOnWrite else { return }
            
            let newValue = wrappedValue
            let adaptor = self.adaptor
            let id = id
            
            Task { try await adaptor.persist(newValue, for: Key(id: id)) }
        }
    }
    
    /// Memberwise initializer.
    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true) {
        self.id = id
        self.saveOnWrite = saveOnWrite
        self.wrappedValue = (try? UserDefaultsAdaptor.shared.loadSynchronously(with: Key(id: id))) ?? wrappedValue
    }
    
    /// Make several modifications to the wrapped value while only persisting it once at the end.
    public mutating func modify(_ modify: (inout Value) -> Void) {
        var value = wrappedValue
        modify(&value)
        
        self.wrappedValue = value
    }
    
    /// Manually persist the value.
    public func persist() async throws {
        try await adaptor.persist(wrappedValue, for: key)
    }
}

@propertyWrapper public struct CryoUbiquitousKeyValue<Value: Codable> {
    struct Key: CryoKey {
        let id: String
    }
    
    /// The ID of the persisted value.
    let id: String
    
    /// The adaptor.
    var adaptor: CryoAdaptor { UbiquitousKeyValueStoreAdaptor.shared }
    
    /// The key for this instance.
    var key: Key { .init(id: id) }
    
    /// Whether to automatically save after each modification.
    let saveOnWrite: Bool
    
    /// The wrapped value.
    public var wrappedValue: Value {
        didSet {
            guard saveOnWrite else { return }
            
            let newValue = wrappedValue
            let adaptor = self.adaptor
            let id = id
            
            Task { try await adaptor.persist(newValue, for: Key(id: id)) }
        }
    }
    
    /// Memberwise initializer.
    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true) {
        self.id = id
        self.saveOnWrite = saveOnWrite
        self.wrappedValue = wrappedValue
        self.wrappedValue = (try? UbiquitousKeyValueStoreAdaptor.shared.loadSynchronously(with: Key(id: id))) ?? wrappedValue
    }
    
    /// Make several modifications to the wrapped value while only persisting it once at the end.
    public mutating func modify(_ modify: (inout Value) -> Void) {
        var value = wrappedValue
        modify(&value)
        
        self.wrappedValue = value
    }
    
    /// Manually persist the value.
    public func persist() async throws {
        try await adaptor.persist(wrappedValue, for: key)
    }
}

@propertyWrapper public struct CryoLocalDocument<Value: Codable> {
    struct Key: CryoKey {
        let id: String
    }
    
    /// The ID of the persisted value.
    let id: String
    
    /// The adaptor.
    var adaptor: CryoAdaptor { DocumentAdaptor.sharedLocal }
    
    /// The key for this instance.
    var key: Key { .init(id: id) }
    
    /// Whether to automatically save after each modification.
    let saveOnWrite: Bool
    
    /// The wrapped value.
    public var wrappedValue: Value {
        didSet {
            guard saveOnWrite else { return }
            
            let newValue = wrappedValue
            let adaptor = self.adaptor
            let id = id
            
            Task { try await adaptor.persist(newValue, for: Key(id: id)) }
        }
    }
    
    /// Memberwise initializer.
    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true) {
        self.id = id
        self.saveOnWrite = saveOnWrite
        self.wrappedValue = (try? DocumentAdaptor.sharedLocal.loadSynchronously(with: Key(id: id))) ?? wrappedValue
    }
    
    /// Make several modifications to the wrapped value while only persisting it once at the end.
    public mutating func modify(_ modify: (inout Value) -> Void) {
        var value = wrappedValue
        modify(&value)
        
        self.wrappedValue = value
    }
    
    /// Manually persist the value.
    public func persist() async throws {
        try await adaptor.persist(wrappedValue, for: key)
    }
}
