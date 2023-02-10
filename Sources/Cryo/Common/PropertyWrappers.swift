
import Foundation

/// Property wrapper that automatically loads and persists values using a configurable adaptor.
///
/// By default, the value is persisted after every modification. If you want to manually persist it instead, set the `saveOnWrite`
/// parameter to `false` and manually call ``CryoPersisted/persist()`` to save the value.
///
/// ```swift
/// struct PersistentCounter {
///     @CryoPersisted("count", adaptor: UserDefaultsAdaptor.shared) var count: Int = 0
/// }
///
/// let counter = PersistentCounter()
/// counter.count += 1
///
/// // Quit and relaunch the app...
/// let counter = PersistentCounter()
/// print(counter.count) // prints "1"
/// ```
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
            self.persist(value: wrappedValue)
        }
    }
    
    /// Create a persisted value wrapper.
    ///
    /// - Parameters:
    ///   - wrappedValue: The wrapped value.
    ///   - id: The identifier to use as a key.
    ///   - saveOnWrite: Whether to automatically persist the value after every modification.
    ///   - adaptor: The adaptor to use for persistence.
    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true, adaptor: any CryoAdaptor) {
        self.id = id
        self.adaptor = adaptor
        self.saveOnWrite = saveOnWrite
        self.wrappedValue = (try? adaptor.loadSynchronously(with: Key(id: id))) ?? wrappedValue
    }
    
    /// Make several modifications to the wrapped value while only persisting it once at the end.
    ///
    /// - Parameter modify: Closure to modify the value before it is persisted.
    public mutating func modify(_ modify: (inout Value) -> Void) {
        var value = wrappedValue
        modify(&value)
        
        self.wrappedValue = value
        if !saveOnWrite { self.persist(value: value) }
    }
    
    /// Manually persist the value.
    public func persist() async throws {
        try await adaptor.persist(wrappedValue, for: key)
    }
    
    /// Persist the value asynchronously.
    func persist(value: Value) {
        _Concurrency.Task { try await adaptor.persist(value, for: key) }
    }
}

/// Property wrapper that automatically loads and persists values using ``UserDefaultsAdaptor/shared``.
///
/// This property wrapper automatically loads and persists its value using the shared `UserDefaults` instance.
/// By default, the value is persisted after every modification. If you want to manually persist it instead, set the `saveOnWrite`
/// parameter to `false` and manually call ``CryoKeyValue/persist()`` to save the value.
///
/// ```swift
/// struct PersistentCounter {
///     @CryoKeyValue("count") var count: Int = 0
/// }
///
/// let counter = PersistentCounter()
/// counter.count += 1
///
/// // Quit and relaunch the app...
/// let counter = PersistentCounter()
/// print(counter.count) // prints "1"
/// ```
@propertyWrapper public struct CryoKeyValue<Value: Codable> {
    /// The ID of the persisted value.
    let id: String
    
    /// The adaptor.
    var adaptor: CryoAdaptor { UserDefaultsAdaptor.shared }
    
    /// The key for this instance.
    var key: CryoNamedKey<Value> { .init(id: id, for: Value.self) }
    
    /// Whether to automatically save after each modification.
    let saveOnWrite: Bool
    
    /// The wrapped value.
    public var wrappedValue: Value {
        didSet {
            guard saveOnWrite else { return }
            self.persist(value: wrappedValue)
        }
    }
    
    /// Create a key-value persisted value wrapper.
    ///
    /// - Parameters:
    ///   - wrappedValue: The wrapped value.
    ///   - id: The identifier to use as a key.
    ///   - saveOnWrite: Whether to automatically persist the value after every modification.
    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true) {
        self.id = id
        self.saveOnWrite = saveOnWrite
        self.wrappedValue = (try? UserDefaultsAdaptor.shared.loadSynchronously(
            with: CryoNamedKey(id: id, for: Value.self))) ?? wrappedValue
    }
    
    /// Make several modifications to the wrapped value while only persisting it once at the end.
    ///
    /// - Parameter modify: Closure to modify the value before it is persisted.
    public mutating func modify(_ modify: (inout Value) -> Void) {
        var value = wrappedValue
        modify(&value)
        
        self.wrappedValue = value
        if !saveOnWrite { self.persist(value: value) }
    }
    
    /// Manually persist the value.
    public func persist() async throws {
        try await adaptor.persist(wrappedValue, for: key)
    }
    
    /// Persist the value asynchronously.
    func persist(value: Value) {
        _Concurrency.Task { try await adaptor.persist(value, for: key) }
    }
}

/// Property wrapper that automatically loads and persists values using ``UbiquitousKeyValueStoreAdaptor/shared``.
///
/// This property wrapper automatically loads and persists its value using the shared `NSUbiquitousKeyValueStore` instance.
/// By default, the value is persisted after every modification. If you want to manually persist it instead, set the `saveOnWrite`
/// parameter to `false` and manually call ``CryoUbiquitousKeyValue/persist()`` to save the value.
///
/// ```swift
/// struct PersistentCounter {
///     @CryoUbiquitousKeyValue("count", saveOnWrite: false) var count: Int = 0
///
///    func persist() { Task { try await _count.persist() } }
/// }
///
/// let counter = PersistentCounter()
/// counter.count += 1
/// // Manually persist
/// counter.persist()
///
/// // Quit and relaunch the app...
/// let counter = PersistentCounter()
/// print(counter.count) // prints "1"
/// ```
@propertyWrapper public struct CryoUbiquitousKeyValue<Value: Codable> {
    /// The ID of the persisted value.
    let id: String
    
    /// The adaptor.
    var adaptor: CryoAdaptor { UbiquitousKeyValueStoreAdaptor.shared }
    
    /// The key for this instance.
    var key: CryoNamedKey<Value> { CryoNamedKey(id: id, for: Value.self) }
    
    /// Whether to automatically save after each modification.
    let saveOnWrite: Bool
    
    /// The wrapped value.
    public var wrappedValue: Value {
        didSet {
            guard saveOnWrite else { return }
            self.persist(value: wrappedValue)
        }
    }
    
    /// Create a ubiquitous key-value persisted value wrapper.
    ///
    /// - Parameters:
    ///   - wrappedValue: The wrapped value.
    ///   - id: The identifier to use as a key.
    ///   - saveOnWrite: Whether to automatically persist the value after every modification.
    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true) {
        self.id = id
        self.saveOnWrite = saveOnWrite
        self.wrappedValue = wrappedValue
        self.wrappedValue = (try? UbiquitousKeyValueStoreAdaptor.shared.loadSynchronously(
            with: CryoNamedKey(id: id, for: Value.self))) ?? wrappedValue
    }
    
    /// Make several modifications to the wrapped value while only persisting it once at the end.
    ///
    /// - Parameter modify: Closure to modify the value before it is persisted.
    public mutating func modify(_ modify: (inout Value) -> Void) {
        var value = wrappedValue
        modify(&value)
        
        self.wrappedValue = value
        if !saveOnWrite { self.persist(value: value) }
    }
    
    /// Manually persist the value.
    public func persist() async throws {
        try await adaptor.persist(wrappedValue, for: key)
    }
    
    /// Persist the value asynchronously.
    func persist(value: Value) {
        _Concurrency.Task { try await adaptor.persist(value, for: key) }
    }
}

/// Property wrapper that automatically loads and persists values using ``DocumentAdaptor/sharedLocal``.
///
/// This property wrapper automatically loads and persists its value a local document.
/// By default, the value is persisted after every modification. If you want to manually persist it instead, set the `saveOnWrite`
/// parameter to `false` and manually call ``CryoLocalDocument/persist()`` to save the value.
///
/// ```swift
/// struct PersistentCounter {
///     @CryoLocalDocument("count") var count: Int = 0
/// }
///
/// let counter = PersistentCounter()
/// counter.count += 1
///
/// // Quit and relaunch the app...
/// let counter = PersistentCounter()
/// print(counter.count) // prints "1"
/// ```
@propertyWrapper public struct CryoLocalDocument<Value: Codable> {
    /// The ID of the persisted value.
    let id: String
    
    /// The adaptor.
    var adaptor: CryoAdaptor { DocumentAdaptor.sharedLocal }
    
    /// The key for this instance.
    var key: CryoNamedKey<Value> { CryoNamedKey(id: id, for: Value.self) }
    
    /// Whether to automatically save after each modification.
    let saveOnWrite: Bool
    
    /// The wrapped value.
    public var wrappedValue: Value {
        didSet {
            guard saveOnWrite else { return }
            self.persist(value: wrappedValue)
        }
    }
    
    /// Create a local document persisted value wrapper.
    ///
    /// - Parameters:
    ///   - wrappedValue: The wrapped value.
    ///   - id: The identifier to use as a key.
    ///   - saveOnWrite: Whether to automatically persist the value after every modification.
    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true) {
        self.id = id
        self.saveOnWrite = saveOnWrite
        self.wrappedValue = (try? DocumentAdaptor.sharedLocal.loadSynchronously(
            with: CryoNamedKey(id: id, for: Value.self))) ?? wrappedValue
    }
    
    /// Make several modifications to the wrapped value while only persisting it once at the end.
    ///
    /// - Parameter modify: Closure to modify the value before it is persisted.
    public mutating func modify(_ modify: (inout Value) -> Void) {
        var value = wrappedValue
        modify(&value)
        
        self.wrappedValue = value
        if !saveOnWrite { self.persist(value: value) }
    }
    
    /// Manually persist the value.
    public func persist() async throws {
        try await adaptor.persist(wrappedValue, for: key)
    }
    
    /// Persist the value asynchronously.
    func persist(value: Value) {
        _Concurrency.Task { try await adaptor.persist(value, for: key) }
    }
}
