
import CloudKit
import Foundation

/// Implementation of ``CryoDatabaseAdaptor`` that persists values in a CloudKit database.
///
/// Values stored by this adaptor must conform to the ``CryoModel`` protocol. For every such type,
/// this adaptor creates a CloudKit table whose name is given by the ``CryoModel/tableName-3pg2z`` property.
///
/// A column is created for every model property that is annotated with either ``CryoColumn`` or ``CryoAsset``.
///
/// - Note: This adaptor does not support synchronous loading via ``CryoSyncronousAdaptor/loadSynchronously(with:)``.
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
public final class CloudKitAdaptor {
    /// The configuration.
    let config: CryoConfig
    
    /// The iCloud container to store to.
    let container: CKContainer
    
    /// The database to store to.
    let database: CKDatabase
    
    /// The unique iCloud record ID for the user.
    var iCloudRecordID: String?
    
    /// Default initializer.
    public init(config: CryoConfig, containerIdentifier: String, database: KeyPath<CKContainer, CKDatabase>) async {
        self.config = config
        
        let container = CKContainer(identifier: containerIdentifier)
        self.container = container
        self.database = container[keyPath: database]
        
        self.iCloudRecordID = await withCheckedContinuation { continuation in
            container.fetchUserRecordID(completionHandler: { (recordID, error) in
                if let error {
                    config.log?(.fault, "error fetching user record id: \(error.localizedDescription)")
                }
                
                continuation.resume(returning: recordID?.recordName)
            })
        }
    }
}

// MARK: CryoDatabaseAdaptor implementation

extension CloudKitAdaptor {
    /// Check for availability of the database.
    public func ensureAvailability() async throws {
        guard self.iCloudRecordID == nil else {
            return
        }
        
        self.iCloudRecordID = await withCheckedContinuation { continuation in
            container.fetchUserRecordID(completionHandler: { (recordID, error) in
                if let error {
                    self.config.log?(.fault, "error fetching user record id: \(error.localizedDescription)")
                }
                
                continuation.resume(returning: recordID?.recordName)
            })
        }
        
        guard self.iCloudRecordID == nil else {
            return
        }
        
        throw CryoError.backendNotAvailable
    }
    
    /// Whether CloudKit is available.
    public var isAvailable: Bool { iCloudRecordID != nil }
    
    public func observeAvailabilityChanges(_ callback: @escaping (Bool) -> Void) {
        // TODO: implement this
    }
}

extension CloudKitAdaptor: CryoDatabaseAdaptor {
    public func createTable<Model: CryoModel>(for model: Model.Type) async throws -> NoOpQuery<Model> {
        NoOpQuery(queryString: "", for: model)
    }
    
    public func select<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> CloudKitSelectQuery<Model> {
        try CloudKitSelectQuery(for: Model.self, id: id, database: database, config: config)
    }
    
    public func insert<Model: CryoModel>(id: String, _ value: Model, replace: Bool = true) async throws -> CloudKitInsertQuery<Model> {
        try CloudKitInsertQuery(id: id, value: value, replace: replace, database: database, config: config)
    }
    
    public func update<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> CloudKitUpdateQuery<Model> {
        try CloudKitUpdateQuery(for: Model.self, id: id, database: database, config: config)
    }
    
    public func delete<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> CloudKitDeleteQuery<Model> {
        try CloudKitDeleteQuery(for: Model.self, id: id, database: database, config: config)
    }
}

extension CloudKitAdaptor {
    static func formatOperator(_ queryOperator: CryoComparisonOperator) -> String {
        switch queryOperator {
        case .equals:
            return "=="
        case .doesNotEqual:
            return "!="
        case .isGreatherThan:
            return ">"
        case .isGreatherThanOrEquals:
            return ">="
        case .isLessThan:
            return "<"
        case .isLessThanOrEquals:
            return "<="
        }
    }
    
    static func placeholderSymbol(for value: CryoQueryValue) -> String {
        switch value {
        case .integer:
            return "%d"
        case .double:
            return "%d"
        default:
            return "%@"
        }
    }
    
    static func queryArgument(for value: CryoQueryValue) -> NSObject {
        switch value {
        case .string(let value):
            return value as NSString
        case .integer(let value):
            return value as NSNumber
        case .double(let value):
            return value as NSNumber
        case .date(let value):
            return NSDate(timeIntervalSinceReferenceDate: value.timeIntervalSinceReferenceDate)
        case .data(let value):
            return value as NSData
        case .asset(let value):
            return value as NSURL
        }
    }
    
    /// Initialize from an NSObject representation.
    static func decodeValue(from nsObject: __CKRecordObjCValue, as type: CryoColumnType) -> _AnyCryoColumnValue? {
        switch type {
        case .integer:
            guard let value = nsObject as? NSNumber else { return nil }
            return Int(truncating: value)
        case .double:
            guard let value = nsObject as? NSNumber else { return nil }
            return Double(truncating: value)
        case .text:
            guard let value = nsObject as? NSString else { return nil }
            return value as String
        case .date:
            guard let value = nsObject as? NSDate else { return nil }
            return Date(timeIntervalSinceReferenceDate: value.timeIntervalSinceReferenceDate)
        case .bool:
            guard let value = nsObject as? NSNumber else { return nil }
            return value != 0
        case .asset:
            guard let value = nsObject as? CKAsset else { return nil }
            return value.fileURL
        case .data:
            guard let value = nsObject as? NSData else { return nil }
            return value as Data
        }
    }
    
    /// The NSObject representation oft his value.
    static func nsObject(from value: _AnyCryoColumnValue, valueType: CryoColumnType) throws -> __CKRecordObjCValue {
        switch value {
        case let url as URL:
            if case .asset = valueType {
                return CKAsset(fileURL: url)
            }
            
            return url.absoluteString as NSString
        case let value as CryoColumnIntValue:
            return value.integerValue as NSNumber
        case let value as CryoColumnDoubleValue:
            return value.doubleValue as NSNumber
        case let value as CryoColumnStringValue:
            return value.stringValue as NSString
        case let value as CryoColumnDateValue:
            return value.dateValue as NSDate
        case let value as CryoColumnDataValue:
            return try value.dataValue as NSData
            
        default:
            return (try JSONEncoder().encode(value)) as NSData
        }
    }
    
    static func check(clause: CryoQueryWhereClause, object: _AnyCryoColumnValue) throws -> Bool {
        switch clause.operation {
        case .equals:
            switch clause.value {
            case .string(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value == object.stringValue
            case .integer(value: let value):
                guard let object = object as? CryoColumnIntValue else { return false }
                return value == object.integerValue
            case .double(value: let value):
                guard let object = object as? CryoColumnDoubleValue else { return false }
                return value == object.doubleValue
            case .date(value: let value):
                guard let object = object as? CryoColumnDateValue else { return false }
                return value == object.dateValue
            case .data(value: let value):
                guard let object = object as? CryoColumnDataValue else { return false }
                return try value == object.dataValue
            case .asset(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value.absoluteString == object.stringValue
            }
        case .doesNotEqual:
            switch clause.value {
            case .string(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value != object.stringValue
            case .integer(value: let value):
                guard let object = object as? CryoColumnIntValue else { return false }
                return value != object.integerValue
            case .double(value: let value):
                guard let object = object as? CryoColumnDoubleValue else { return false }
                return value != object.doubleValue
            case .date(value: let value):
                guard let object = object as? CryoColumnDateValue else { return false }
                return value != object.dateValue
            case .data(value: let value):
                guard let object = object as? CryoColumnDataValue else { return false }
                return try value != object.dataValue
            case .asset(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value.absoluteString != object.stringValue
            }
        case .isGreatherThan:
            switch clause.value {
            case .string(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value > object.stringValue
            case .integer(value: let value):
                guard let object = object as? CryoColumnIntValue else { return false }
                return value > object.integerValue
            case .double(value: let value):
                guard let object = object as? CryoColumnDoubleValue else { return false }
                return value > object.doubleValue
            case .date(value: let value):
                guard let object = object as? CryoColumnDateValue else { return false }
                return value > object.dateValue
            case .asset(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value.absoluteString > object.stringValue
            case .data:
                return false
            }
        case .isGreatherThanOrEquals:
            switch clause.value {
            case .string(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value >= object.stringValue
            case .integer(value: let value):
                guard let object = object as? CryoColumnIntValue else { return false }
                return value >= object.integerValue
            case .double(value: let value):
                guard let object = object as? CryoColumnDoubleValue else { return false }
                return value >= object.doubleValue
            case .date(value: let value):
                guard let object = object as? CryoColumnDateValue else { return false }
                return value >= object.dateValue
            case .asset(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value.absoluteString >= object.stringValue
            case .data:
                return false
            }
        case .isLessThan:
            switch clause.value {
            case .string(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value < object.stringValue
            case .integer(value: let value):
                guard let object = object as? CryoColumnIntValue else { return false }
                return value < object.integerValue
            case .double(value: let value):
                guard let object = object as? CryoColumnDoubleValue else { return false }
                return value < object.doubleValue
            case .date(value: let value):
                guard let object = object as? CryoColumnDateValue else { return false }
                return value < object.dateValue
            case .asset(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value.absoluteString < object.stringValue
            case .data:
                return false
            }
        case .isLessThanOrEquals:
            switch clause.value {
            case .string(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value <= object.stringValue
            case .integer(value: let value):
                guard let object = object as? CryoColumnIntValue else { return false }
                return value <= object.integerValue
            case .double(value: let value):
                guard let object = object as? CryoColumnDoubleValue else { return false }
                return value <= object.doubleValue
            case .date(value: let value):
                guard let object = object as? CryoColumnDateValue else { return false }
                return value <= object.dateValue
            case .asset(value: let value):
                guard let object = object as? CryoColumnStringValue else { return false }
                return value.absoluteString <= object.stringValue
            case .data:
                return false
            }
        }
    }
}

internal extension CryoQueryValue {
    init?(value: __CKRecordObjCValue) {
        switch value {
        case let value as NSString:
            self = .string(value: value as String)
        case let value as NSNumber:
            self = .double(value: value.doubleValue)
        case let value as NSDate:
            self = .date(value: value as Date)
        case let value as NSData:
            self = .data(value: value as Data)
        case let value as CKAsset:
            if let url = value.fileURL {
                self = .asset(value: url)
                break
            }
            
            fallthrough
        default:
            return nil
        }
    }
    
    var recordValue: __CKRecordObjCValue {
        switch self {
        case .string(let value):
            return value as NSString
        case .integer(let value):
            return value as NSNumber
        case .double(let value):
            return value as NSNumber
        case .date(let value):
            return value as NSDate
        case .data(let value):
            return value as NSData
        case .asset(let value):
            return CKAsset(fileURL: value)
        }
    }
}
