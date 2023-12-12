
import Foundation

public final class CryoContext {
    /// The database adaptor used for storage.
    let adaptor: SynchronizedStore
    
    /// Cache of local instances.
    var managedInstances: [String: any CryoClassModel] = [:]
    
    /// Create a context.
    public init(adaptor: SynchronizedStore) {
        self.adaptor = adaptor
    }
}

// MARK: Public API

extension CryoContext {
    /// Register a new model.
    public func register<Model: CryoClassModel>(model: Model.Type) async throws {
        
    }
    
    /// Register a new value.
    public func manage<Model: CryoClassModel>(_ value: Model) async throws {
        try await adaptor.insert(value, replace: false).execute()
    }
}

// MARK: Private API

extension CryoContext {
    /// React to an external change notification from the store.
    func onExternalChangeNotificationReceived() {
        
    }
}

// MARK: CryoDatabaseAdaptor for CryoContext

extension CryoContext: CryoDatabaseAdaptor {
    public func createTable<Model: CryoModel>(for model: Model.Type) async throws -> any CryoCreateTableQuery<Model> {
        try await adaptor.createTable(for: model)
    }
    
    public func select<Model: CryoModel>(id: String? = nil, from model: Model.Type) throws -> any CryoSelectQuery<Model> {
        WrappedSelectQuery(query: try adaptor.select(id: id, from: model)) { result in
            
        }
    }
    
    public func insert<Model: CryoModel>(_ value: Model, replace: Bool = true) throws -> any CryoInsertQuery<Model> {
        WrappedInsertQuery(query: try adaptor.insert(value, replace: replace)) { result in
            
        }
    }
    
    public func update<Model: CryoModel>(id: String? = nil, from modelType: Model.Type) throws -> any CryoUpdateQuery<Model> {
        WrappedUpdateQuery(query: try adaptor.update(id: id, from: modelType)) { result in
        }
    }
    
    public func delete<Model: CryoModel>(id: String? = nil, from model: Model.Type) throws -> any CryoDeleteQuery<Model> {
        WrappedDeleteQuery(query: try adaptor.delete(id: id, from: model)) { result in
        }
    }
}

public protocol CryoClassModel: AnyObject, CryoModel, Codable, ObservableObject {
    /// The name of the table representing this model.
    ///
    /// By default, the type name is used as the table name.
    static var tableName: String { get }
    
    /// The ID of this model.
    var id: String { get }
}

// MARK: Default values

public extension CryoContext {
    /// Create a default `Int` value.
    func defaultValue(for: Int.Type) -> Int { 0 }
    
    /// Create a default `UInt` value.
    func defaultValue(for: UInt.Type) -> UInt { 0 }
    
    /// Create a default `Boolean` value.
    func defaultValue(for: Bool.Type) -> Bool { false }
    
    /// Create a default `Double` value.
    func defaultValue(for: Double.Type) -> Double { 0 }
    
    /// Create a default `String` value.
    func defaultValue(for: String.Type) -> String { "" }
    
    /// Create a default `Date` value.
    func defaultValue(for: Date.Type) -> Date { Date() }
    
    /// Create a default `Data` value.
    func defaultValue(for: Data.Type) -> Data { Data() }
    
    /// Create a default `URL` value.
    func defaultValue(for: URL.Type) -> URL { URL(fileURLWithPath: "") }
    
    /// Create a default `Optional` value.
    func defaultValue<T>(for: Optional<T>.Type) -> Optional<T> { nil }
    
    /// Create a default `Codable` value.
    func defaultValue<T>(for: T.Type) -> T where T: Codable { try! T(from: EmptyDecoder()) }
}
