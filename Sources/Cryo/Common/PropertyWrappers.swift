import Foundation

private final class PersistenceQueue: @unchecked Sendable {
    private let lock = NSLock()
    private var tail: Task<Void, Never>?
    private var errors: [Error] = []
    private var autoSaveSuppressed = false

    private func withLock<T>(_ body: () -> T) -> T {
        lock.lock(); defer { lock.unlock() }
        return body()
    }

    func suppressAutoSave() {
        withLock { autoSaveSuppressed = true }
    }

    func enableAutoSave() {
        withLock { autoSaveSuppressed = false }
    }

    var permitsAutoSave: Bool {
        withLock { !autoSaveSuppressed }
    }

    func enqueue<Value, Key>(value: Value, key: Key, adaptor: any CryoAdaptor,
                             onError: @escaping (Error) -> Void) -> Task<Result<Void, Error>, Never>
        where Key: CryoKey, Key.Value == Value
    {
        lock.lock()
        let previous = tail
        let operation = Task<Result<Void, Error>, Never> {
            await previous?.value
            do {
                try await adaptor.persist(value, for: key)
                return .success(())
            } catch {
                self.withLock { self.errors.append(error) }
                onError(error)
                return .failure(error)
            }
        }
        tail = Task { _ = await operation.value }
        lock.unlock()
        return operation
    }

    func flush() async throws {
        let pending = withLock { tail }
        await pending?.value
        let error = withLock { errors.isEmpty ? nil : errors.removeFirst() }
        if let error { throw error }
    }
}

/// Property wrapper that loads synchronously and persists changes in order through a configurable adaptor.
@propertyWrapper public struct CryoPersisted<Value: Codable> {
    struct Key: CryoKey { let id: String }

    let id: String
    let adaptor: any CryoSynchronousAdaptor
    let saveOnWrite: Bool
    private let queue: PersistenceQueue
    private let onError: (Error) -> Void
    var key: Key { .init(id: id) }

    public var wrappedValue: Value {
        didSet {
            guard saveOnWrite, queue.permitsAutoSave else { return }
            _ = enqueue(value: wrappedValue)
        }
    }

    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true,
                adaptor: any CryoSynchronousAdaptor, config: CryoConfig = .init(),
                onError: ((Error) -> Void)? = nil) {
        self.id = id
        self.adaptor = adaptor
        self.saveOnWrite = saveOnWrite
        self.queue = PersistenceQueue()
        self.onError = onError ?? { error in
            config.log?(.error, "[CryoPersisted] failed to persist \(id): \(error)")
        }
        do {
            self.wrappedValue = try adaptor.loadSynchronously(with: Key(id: id)) ?? wrappedValue
        } catch {
            self.wrappedValue = wrappedValue
            queue.suppressAutoSave()
            self.onError(error)
        }
    }

    public init(defaultValue: Value, _ id: String, saveOnWrite: Bool = true,
                adaptor: any CryoSynchronousAdaptor, config: CryoConfig = .init(),
                onError: ((Error) -> Void)? = nil) {
        self.init(wrappedValue: defaultValue, id, saveOnWrite: saveOnWrite,
                  adaptor: adaptor, config: config, onError: onError)
    }

    public mutating func modify(_ modify: (inout Value) -> Void) {
        var value = wrappedValue
        modify(&value)
        wrappedValue = value
        if !saveOnWrite { _ = enqueue(value: value) }
    }

    /// Persist the current value after all writes already queued for this wrapper.
    public func persist() async throws {
        queue.enableAutoSave()
        try await enqueue(value: wrappedValue).value.get()
    }

    /// Wait for all writes that were queued when this method was called.
    public func flush() async throws { try await queue.flush() }

    private func enqueue(value: Value) -> Task<Result<Void, Error>, Never> {
        queue.enqueue(value: value, key: key, adaptor: adaptor, onError: onError)
    }
}

/// Property wrapper backed by ``UserDefaultsAdaptor/shared``.
@propertyWrapper public struct CryoKeyValue<Value: Codable> {
    private var storage: CryoPersisted<Value>
    public var wrappedValue: Value {
        get { storage.wrappedValue }
        set { storage.wrappedValue = newValue }
    }

    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true,
                onError: ((Error) -> Void)? = nil) {
        storage = .init(wrappedValue: wrappedValue, id, saveOnWrite: saveOnWrite,
                        adaptor: UserDefaultsAdaptor.shared, onError: onError)
    }

    public init(defaultValue: Value, _ id: String, saveOnWrite: Bool = true,
                onError: ((Error) -> Void)? = nil) {
        self.init(wrappedValue: defaultValue, id, saveOnWrite: saveOnWrite, onError: onError)
    }

    public mutating func modify(_ modify: (inout Value) -> Void) { storage.modify(modify) }
    public func persist() async throws { try await storage.persist() }
    public func flush() async throws { try await storage.flush() }
}

/// Property wrapper backed by ``UbiquitousKeyValueStoreAdaptor/shared``.
@propertyWrapper public struct CryoUbiquitousKeyValue<Value: Codable> {
    private var storage: CryoPersisted<Value>
    public var wrappedValue: Value {
        get { storage.wrappedValue }
        set { storage.wrappedValue = newValue }
    }

    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true,
                onError: ((Error) -> Void)? = nil) {
        storage = .init(wrappedValue: wrappedValue, id, saveOnWrite: saveOnWrite,
                        adaptor: UbiquitousKeyValueStoreAdaptor.shared, onError: onError)
    }

    public init(defaultValue: Value, _ id: String, saveOnWrite: Bool = true,
                onError: ((Error) -> Void)? = nil) {
        self.init(wrappedValue: defaultValue, id, saveOnWrite: saveOnWrite, onError: onError)
    }

    public mutating func modify(_ modify: (inout Value) -> Void) { storage.modify(modify) }
    public func persist() async throws { try await storage.persist() }
    public func flush() async throws { try await storage.flush() }
}

/// Property wrapper backed by ``DocumentAdaptor/sharedLocal``.
@propertyWrapper public struct CryoLocalDocument<Value: Codable> {
    private var storage: CryoPersisted<Value>
    public var wrappedValue: Value {
        get { storage.wrappedValue }
        set { storage.wrappedValue = newValue }
    }

    public init(wrappedValue: Value, _ id: String, saveOnWrite: Bool = true,
                onError: ((Error) -> Void)? = nil) {
        storage = .init(wrappedValue: wrappedValue, id, saveOnWrite: saveOnWrite,
                        adaptor: DocumentAdaptor.sharedLocal, onError: onError)
    }

    public init(defaultValue: Value, _ id: String, saveOnWrite: Bool = true,
                onError: ((Error) -> Void)? = nil) {
        self.init(wrappedValue: defaultValue, id, saveOnWrite: saveOnWrite, onError: onError)
    }

    public mutating func modify(_ modify: (inout Value) -> Void) { storage.modify(modify) }
    public func persist() async throws { try await storage.persist() }
    public func flush() async throws { try await storage.flush() }
}
