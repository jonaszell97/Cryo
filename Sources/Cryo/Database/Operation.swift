
import CloudKit
import Foundation

public enum DatabaseOperationType: String, Codable, CryoColumnStringValue {
    /// An insert operation.
    case insert
    
    /// An  update operation.
    case update
    
    /// A deletion operation.
    case delete
}

public struct DatabaseOperationValue: Codable, CryoColumnDataValue {
    /// The name of the column.
    let columnName: String
    
    /// The value.
    let value: CryoQueryValue
}

public struct DatabaseOperation {
    /// The operation type.
    let type: DatabaseOperationType
    
    /// The date of the operation.
    let date: Date
    
    /// The table name.
    let tableName: String
    
    /// The record ID.
    let rowId: String
    
    /// The optional data.
    let data: [DatabaseOperationValue]
    
    /// Create an insert operation.
    static func insert(tableName: String, id: String, data: [DatabaseOperationValue]) -> DatabaseOperation {
        .init(type: .insert, date: .now, tableName: tableName, rowId: id, data: data)
    }
    
    /// Create an insert operation.
    public static func insert<Model: CryoModel>(tableName: String, id: String, model: Model) throws -> DatabaseOperation {
        .init(type: .insert, date: .now, tableName: tableName, rowId: id, data: try model.codableData)
    }
    
    /// Create an update operation.
    static func update(tableName: String, id: String, data: [DatabaseOperationValue]) -> DatabaseOperation {
        .init(type: .update, date: .now, tableName: tableName, rowId: id, data: data)
    }
    
    /// Create an update operation.
    public static func update<Model: CryoModel>(tableName: String, id: String, model: Model) throws -> DatabaseOperation {
        .init(type: .update, date: .now, tableName: tableName, rowId: id, data: try model.codableData)
    }
    
    /// Create a delete operation.
    public static func delete(tableName: String, id: String) -> DatabaseOperation {
        .init(type: .delete, date: .now, tableName: tableName, rowId: id, data: [])
    }
    
    /// Create a delete operation.
    public static func delete(tableName: String) -> DatabaseOperation {
        .init(type: .delete, date: .now, tableName: tableName, rowId: "", data: [])
    }
    
    /// Create a delete operation.
    public static func deleteAll() -> DatabaseOperation {
        .init(type: .delete, date: .now, tableName: "", rowId: "", data: [])
    }
    
    internal init(type: DatabaseOperationType, date: Date, tableName: String, rowId: String, data: [DatabaseOperationValue]) {
        self.type = type
        self.date = date
        self.tableName = tableName
        self.rowId = rowId
        self.data = data
    }
}

// MARK: Conformances

extension DatabaseOperation: Codable {
    enum CodingKeys: String, CodingKey {
        case type, date, tableName, rowId, data
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(type, forKey: .type)
        try container.encode(date, forKey: .date)
        try container.encode(tableName, forKey: .tableName)
        try container.encode(rowId, forKey: .rowId)
        try container.encode(data, forKey: .data)
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            type: try container.decode(DatabaseOperationType.self, forKey: .type),
            date: try container.decode(Date.self, forKey: .date),
            tableName: try container.decode(String.self, forKey: .tableName),
            rowId: try container.decode(String.self, forKey: .rowId),
            data: try container.decode([DatabaseOperationValue].self, forKey: .data)
        )
    }
}

// MARK: Utility

internal extension CryoModel {
    var codableData: [DatabaseOperationValue] {
        get throws {
            let schema = Self.schema
            var data: [DatabaseOperationValue] = []
            
            for column in schema {
                data.append(.init(columnName: column.columnName, value: try .init(value: column.getValue(self))))
            }
            
            return data
        }
    }
}

internal extension CryoQueryValue {
    init (value: _AnyCryoColumnValue) throws {
        switch value {
        case let url as URL:
            self = .string(value: url.absoluteString)
        case let value as CryoColumnIntValue:
            self = .integer(value: Int(value.integerValue))
        case let value as CryoColumnDoubleValue:
            self = .double(value: value.doubleValue)
        case let value as CryoColumnStringValue:
            self = .string(value: value.stringValue)
        case let value as CryoColumnDateValue:
            self = .date(value: value.dateValue)
        case let value as CryoColumnDataValue:
            self = .data(value: try value.dataValue)
        default:
            self = .data(value: try JSONEncoder().encode(value))
        }
    }
    
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
    
    var objcValue: __CKRecordObjCValue {
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
