
import CloudKit
import Foundation

internal enum DatabaseOperation: CryoColumnDataValue {
    /// An insert operation.
    case insert(date: Date, tableName: String, rowId: String, data: [DatabaseOperationValue])
    
    /// An  update operation.
    case update(date: Date, tableName: String, rowId: String?,
                setClauses: [CryoQuerySetClause],
                whereClauses: [CryoQueryWhereClause])
    
    /// A deletion operation.
    case delete(date: Date, tableName: String, rowId: String?, whereClauses: [CryoQueryWhereClause])
}

internal struct DatabaseOperationValue: Codable, CryoColumnDataValue {
    /// The name of the column.
    let columnName: String
    
    /// The value.
    let value: CryoQueryValue
}

extension CryoInsertQuery {
    /// The database operation for this query.
    internal var operation: DatabaseOperation {
        get async throws {
            var data = [DatabaseOperationValue]()
            let schema = CryoSchemaManager.shared.schema(for: Model.self)
            
            for column in schema.columns {
                data.append(.init(columnName: column.columnName, value: try .init(value: column.getValue(self.value))))
            }
            
            return .insert(date: .now, tableName: Model.tableName, rowId: self.id, data: data)
        }
    }
}

extension CryoUpdateQuery {
    /// The database operation for this query.
    internal var operation: DatabaseOperation {
        get async throws {
            .update(date: .now, tableName: Model.tableName, rowId: self.id,
                    setClauses: self.setClauses, whereClauses: self.whereClauses)
        }
    }
}

extension CryoDeleteQuery {
    /// The database operation for this query.
    internal var operation: DatabaseOperation {
        get async throws {
            .delete(date: .now, tableName: Model.tableName, rowId: self.id, whereClauses: self.whereClauses)
        }
    }
}

// MARK: Conformances

extension DatabaseOperation: CustomStringConvertible {
    public var description: String {
        switch self {
        case .insert(_, let tableName, _, let data):
            return "INSERT INTO \(tableName) (\(data.map(\.columnName).joined(separator: ", "))) VALUES (\(data.map { "\($0.value)" }.joined(separator: ", ")))"
        case .update(_, let tableName, _, let setClauses, let whereClauses):
            return "UPDATE \(tableName) SET \(setClauses.map { "\($0.value)" }.joined(separator: ", ")) WHERE \(whereClauses.map { "\($0.value)" }.joined(separator: ", "))"
        case .delete(_, let tableName, _, let whereClauses):
            return "DELETE FROM \(tableName) WHERE \(whereClauses.map { "\($0.value)" }.joined(separator: ", "))"
        }
    }
}

extension DatabaseOperation: Codable {
    enum CodingKeys: String, CodingKey {
        case insert, update, delete
    }
    
    enum insertCodingKeys: CodingKey {
        case _0, _1, _2, _3
    }
    enum updateCodingKeys: CodingKey {
        case _0, _1, _2, _3, _4
    }
    enum deleteCodingKeys: CodingKey {
        case _0, _1, _2, _3
    }
    
    var codingKey: CodingKeys {
        switch self {
        case .insert: return .insert
        case .update: return .update
        case .delete: return .delete
        }
    }
    
    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .insert(let date, let tableName, let rowId, let data):
            var nestedContainer = container.nestedContainer(keyedBy: insertCodingKeys.self, forKey: .insert)
            try nestedContainer.encode(date, forKey: ._0)
            try nestedContainer.encode(tableName, forKey: ._1)
            try nestedContainer.encode(rowId, forKey: ._2)
            try nestedContainer.encode(data, forKey: ._3)
        case .update(let date, let tableName, let rowId, let setClauses, let whereClauses):
            var nestedContainer = container.nestedContainer(keyedBy: updateCodingKeys.self, forKey: .update)
            try nestedContainer.encode(date, forKey: ._0)
            try nestedContainer.encode(tableName, forKey: ._1)
            try nestedContainer.encode(rowId, forKey: ._2)
            try nestedContainer.encode(setClauses, forKey: ._3)
            try nestedContainer.encode(whereClauses, forKey: ._4)
        case .delete(let date, let tableName, let rowId, let whereClauses):
            var nestedContainer = container.nestedContainer(keyedBy: deleteCodingKeys.self, forKey: .delete)
            try nestedContainer.encode(date, forKey: ._0)
            try nestedContainer.encode(tableName, forKey: ._1)
            try nestedContainer.encode(rowId, forKey: ._2)
            try nestedContainer.encode(whereClauses, forKey: ._3)
        }
    }
    
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch container.allKeys.first {
        case .insert:
            let nestedContainer = try container.nestedContainer(keyedBy: insertCodingKeys.self, forKey: .insert)
            let (date, tableName, rowId, data): (Date, String, String, Array<DatabaseOperationValue>) = (
                try nestedContainer.decode(Date.self, forKey: ._0),
                try nestedContainer.decode(String.self, forKey: ._1),
                try nestedContainer.decode(String.self, forKey: ._2),
                try nestedContainer.decode(Array<DatabaseOperationValue>.self, forKey: ._3)
            )
            self = .insert(date: date, tableName: tableName, rowId: rowId, data: data)
        case .update:
            let nestedContainer = try container.nestedContainer(keyedBy: updateCodingKeys.self, forKey: .update)
            let (date, tableName, rowId, setClauses, whereClauses): (Date, String, String, Array<CryoQuerySetClause>, Array<CryoQueryWhereClause>) = (
                try nestedContainer.decode(Date.self, forKey: ._0),
                try nestedContainer.decode(String.self, forKey: ._1),
                try nestedContainer.decode(String.self, forKey: ._2),
                try nestedContainer.decode(Array<CryoQuerySetClause>.self, forKey: ._3),
                try nestedContainer.decode(Array<CryoQueryWhereClause>.self, forKey: ._4)
            )
            self = .update(date: date, tableName: tableName, rowId: rowId, setClauses: setClauses, whereClauses: whereClauses)
        case .delete:
            let nestedContainer = try container.nestedContainer(keyedBy: deleteCodingKeys.self, forKey: .delete)
            let (date, tableName, rowId, whereClauses): (Date, String, String, Array<CryoQueryWhereClause>) = (
                try nestedContainer.decode(Date.self, forKey: ._0),
                try nestedContainer.decode(String.self, forKey: ._1),
                try nestedContainer.decode(String.self, forKey: ._2),
                try nestedContainer.decode(Array<CryoQueryWhereClause>.self, forKey: ._3)
            )
            self = .delete(date: date, tableName: tableName, rowId: rowId, whereClauses: whereClauses)
        default:
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: container.codingPath,
                    debugDescription: "Unabled to decode enum."
                )
            )
        }
    }
}

// MARK: Utility

internal extension CryoModel {
    var codableData: [DatabaseOperationValue] {
        get throws {
            let schema = Self.schema
            var data: [DatabaseOperationValue] = []
            
            for column in schema.columns {
                data.append(.init(columnName: column.columnName, value: try .init(value: column.getValue(self))))
            }
            
            return data
        }
    }
}
