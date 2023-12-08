
import Foundation

/*
/// Protocol for types that can be persisted using CloudKit.
///
/// If you want to persist a type using ``CloudKitAdaptor``, you need to implement a
/// conformance to this protocol. `CloudKitAdaptor` will persist all of the type's properties
/// that are annotated with either the ``CryoColumn`` or the ``CryoAsset`` property wrappers.
///
/// Each such property will be assigned a column in the database table.
///
/// Take the following model definition as an example:
///
/// ```swift
/// struct Message: CryoModel {
///     @CryoColumn var content: String
///     @CryoColumn var created: Date
///     @CryoAsset var attachment
/// }
///
/// try await adaptor.persist(Message(content: "Hello", created: Date.now, attachment: /*...*/),
///                           with: CryoNamedKey(id: "1", for: Message.self))
/// try await adaptor.persist(Message(content: "Hi", created: Date.now, attachment: /*...*/),
///                           with: CryoNamedKey(id: "2", for: Message.self))
/// try await adaptor.persist(Message(content: "How are you?", created: Date.now, attachment: /*...*/),
///                           with: CryoNamedKey(id: "3", for: Message.self))
///
/// ```
///
/// Based on this definition, `CloudKitAdaptor` will create a table in CloudKIt named `Message`
/// with the following structure:
///
/// | ID  | content: `NSString` | created: `NSDate` | attachment: `NSURL` |
/// | ---- | ---------- | ---------- | -------------- |
/// | 1   | "Hello"  | YYYY-MM-DD | /... |
/// | 2   | "Hi"  | YYYY-MM-DD | /... |
/// | 3   | "How are you?"  | YYYY-MM-DD | /... |
public protocol CryoModel: Codable {
    /// The name of the table representing this model.
    ///
    /// By default, the type name is used as the table name.
    static var tableName: String { get }
    
    /// The ID of this model.
    var id: String { get }
}

extension CryoModel {
    /// The name of the table representing this model.
    public static var tableName: String { "\(Self.self)" }
}

internal final class CryoSchemaManager {
    /// The schemas mapped by object type.
    var schemas: [ObjectIdentifier: CryoSchema]
    
    /// The schemas mapped by table name.
    var schemasByName: [String: CryoSchema]
    
    /// The shared instance.
    static let shared = CryoSchemaManager()
    
    /// Create a schema manager.
    init() {
        self.schemas = [:]
        self.schemasByName = [:]
    }
    
    /// Create a schema if it does not exist.
    @MainActor func createSchema<Model: CryoModel>(for model: Model.Type) {
        let schemaKey = ObjectIdentifier(Model.self)
        guard self.schemas[schemaKey] == nil else {
            return
        }
        
        let schema = Model.schema
        self.schemas[schemaKey] = schema
        self.schemasByName[Model.tableName] = schema
    }
    
    /// Create a schema if it does not exist.
    @MainActor func createSchema(for modelType: any CryoModel.Type) {
        let schemaKey = ObjectIdentifier(modelType)
        guard self.schemas[schemaKey] == nil else {
            return
        }
        
        let schema = modelType.schema
        self.schemas[schemaKey] = schema
        self.schemasByName[modelType.tableName] = schema
    }
    
    /// Find a schema.
    func schema<Model: CryoModel>(for model: Model.Type) -> CryoSchema {
        let schemaKey = ObjectIdentifier(Model.self)
        guard let schema = self.schemas[schemaKey] else {
            fatalError("schema for model \(model.tableName) was not initialized, did you forget a Create Table operation?")
        }
        
        return schema
    }
    
    /// Find or create a schema.
    func schema(for modelType: any CryoModel.Type) -> CryoSchema {
        let schemaKey = ObjectIdentifier(modelType)
        guard let schema = self.schemas[schemaKey] else {
            fatalError("schema for model \(modelType.tableName) was not initialized, did you forget a Create Table operation?")
        }
        
        return schema
    }
    
    /// Find or create a schema.
    func schema(tableName: String) -> CryoSchema? {
        schemasByName[tableName]
    }
}

internal enum CryoSchemaColumn {
    /// A value column.
    case value(columnName: String, type: CryoColumnType, getValue: (any CryoModel) -> _AnyCryoColumnValue)
    
    /// A one-to-one relationship.
    case oneToOneRelation(columnName: String, modelType: any CryoModel.Type, getValue: (any CryoModel) -> _AnyCryoColumnValue)
}

extension CryoSchemaColumn {
    var columnName: String {
        switch self {
        case .value(let columnName, _, _):
            return columnName
        case .oneToOneRelation(let columnName, _, _):
            return columnName
        }
    }
    
    var getValue: (any CryoModel) -> _AnyCryoColumnValue {
        switch self {
        case .value(_, _, let getValue):
            return getValue
        case .oneToOneRelation(_, _, let getValue):
            return getValue
        }
    }
}

internal struct CryoSchema {
    /// The meta type.
    let `self`: any CryoModel.Type
    
    /// The columns of this type.
    var columns: [CryoSchemaColumn] = []
    
    /// Create a value of this model type from the given data dictionary.
    let create: ([String: _AnyCryoColumnValue]) throws -> any CryoModel
}*/
