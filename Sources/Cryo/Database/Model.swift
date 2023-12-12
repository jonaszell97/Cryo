
import Foundation

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
public protocol CryoModel: Codable, _AnyCryoColumnValue {
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

/// Property wrapper for columns in a ``CryoModel``.
///
/// Each property that is annotated as a `CryoColumn` will receive a column in the database table
/// of the containing ``CryoModel``. The column name is equal to the name of the property.
///
/// This property wrapper supports values of type `Int`, `Bool`, `Double`, `Float`, `String`,
/// `Date`, `URL`, and `Data`.
///
/// ```swift
/// struct Message: CryoModel {
///     @CryoColumn var content: String
///     @CryoColumn var created: Date
/// }
/// ```
@propertyWrapper public struct CryoColumn<Value: _AnyCryoColumnValue> {
    /// The wrapped, persistable value.
    public var wrappedValue: Value
    
    /// Create a column wrapper.
    public init(wrappedValue: Value) {
        self.wrappedValue = wrappedValue
    }
}

extension CryoColumn: Codable {
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(wrappedValue)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.init(wrappedValue: try container.decode(Value.self))
    }
}

@propertyWrapper public struct CryoOneToOne<Value: CryoModel> {
    /// The wrapped value that is referenced by this column.
    public var wrappedValue: Value
    
    /// Create a one-to-one relationship.
    public init(wrappedValue: Value) {
        self.wrappedValue = wrappedValue
    }
}

extension CryoOneToOne: Codable {
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(wrappedValue)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.init(wrappedValue: try container.decode(Value.self))
    }
}

@propertyWrapper public struct CryoOneToMany<Value: CryoModel> {
    /// The wrapped value that is referenced by this column.
    public var wrappedValue: [Value]
    
    /// Create a one-to-one relationship.
    public init(wrappedValue: [Value]) {
        self.wrappedValue = wrappedValue
    }
}

extension CryoOneToMany: Codable {
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(wrappedValue)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.init(wrappedValue: try container.decode([Value].self))
    }
}

/// Property wrapper for assets in a ``CryoModel``.
///
/// Each property that is annotated as a `CryoAsset` will receive a column in the database table
/// of the containing ``CryoModel``. The column name is equal to the name of the property.
///
/// When persisting a model with a stored `CryoAsset`, the contents of the file at the ``CryoAsset/wrappedValue``
/// URL will be uploaded as a CloudKit asset. For values fetched from CloudKit, this URL will point to
/// the local asset file created by CloudKit.
@propertyWrapper public struct CryoAsset {
    /// The URL of the asset.
    public var wrappedValue: URL
    
    /// Create an asset wrapper.
    public init(wrappedValue: URL) {
        self.wrappedValue = wrappedValue
    }
}

extension CryoAsset: Codable {
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(wrappedValue)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.init(wrappedValue: try container.decode(URL.self))
    }
}

// MARK: Model reflection

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

public enum CryoSchemaColumn {
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

public struct CryoSchema {
    /// The meta type.
    public let `self`: any CryoModel.Type
    
    /// The columns of this type.
    public let columns: [CryoSchemaColumn]
    
    /// Create a value of this model type from the given data dictionary.
    public let create: ([String: _AnyCryoColumnValue]) throws -> any CryoModel
    
    public init(self this: any CryoModel.Type, columns: [CryoSchemaColumn],
                create: @escaping ([String : _AnyCryoColumnValue]) throws -> any CryoModel) {
        self.`self` = this
        self.columns = columns
        self.create = create
    }
}

internal extension CryoModel {
    static var schema: CryoSchema {
        
        // Create an empty instance and find columns from it
        let emptyInstance = try! Self(from: EmptyDecoder())
        let mirror = Mirror(reflecting: emptyInstance)
        var foundId = false
        var columns: [CryoSchemaColumn] = []
        
        for child in mirror.children {
            guard
                let label = child.label,
                label.hasPrefix("_")
            else {
                continue
            }
            
            let name = "\(label.dropFirst())"
            guard !name.isEmpty else {
                continue
            }
            
            foundId = foundId || label == "_id"
            
            let childMirror = Mirror(reflecting: child.value)
            let wrappedValue = childMirror.children.first {
                $0.label == "wrappedValue"
            }
            
            guard let wrappedValue else {
                continue
            }
            
            let columnType: CryoColumnType
            let childTypeName = "\(childMirror.subjectType)"
            
            let wrappedValueMirror = Mirror(reflecting: wrappedValue.value)
            let column: CryoSchemaColumn
            
            if childTypeName.starts(with: "CryoOneToOne") {
                let extractValue: (any CryoModel) -> _AnyCryoColumnValue = { this in
                    let mirror = Mirror(reflecting: this)
                    let child = mirror.children.first { $0.label == label }!
                    let childMirror = Mirror(reflecting: child.value)
                    let wrappedValue = childMirror.children.first { $0.label == "wrappedValue" }!.value
                    
                    return (wrappedValue as! CryoModel).id
                }
                
                column = .oneToOneRelation(columnName: name,
                                           modelType: wrappedValueMirror.subjectType as! CryoModel.Type,
                                           getValue: extractValue)
            }
            else {
                let extractValue: (any CryoModel) -> _AnyCryoColumnValue = { this in
                    let mirror = Mirror(reflecting: this)
                    let child = mirror.children.first { $0.label == label }!
                    let childMirror = Mirror(reflecting: child.value)
                    let wrappedValue = childMirror.children.first { $0.label == "wrappedValue" }!.value
                    
                    return wrappedValue as! _AnyCryoColumnValue
                }
                
                if childTypeName.starts(with: "CryoColumn") {
                    switch wrappedValueMirror.subjectType {
                    case is CryoColumnIntValue.Type: columnType = .integer
                    case is CryoColumnDoubleValue.Type: columnType = .double
                    case is CryoColumnStringValue.Type: columnType = .text
                    case is CryoColumnDateValue.Type: columnType = .date
                    case is CryoColumnDataValue.Type: columnType = .data
                    default:
                        fatalError("\(wrappedValueMirror.subjectType) is not a valid type for a CryoColumn")
                    }
                }
                else if childTypeName.starts(with: "CryoAsset") {
                    columnType = .asset
                }
                else {
                    continue
                }
                
                column = .value(columnName: name, type: columnType, getValue: extractValue)
            }
            
            columns.append(column)
        }
        
        guard foundId else {
            fatalError("CryoModel must contain property `@CryoColumn var id: String`")
        }
        
        return CryoSchema(self: Self.self, columns: columns) {
            try Self(from: CryoModelDecoder(data: $0))
        }
    }
}
