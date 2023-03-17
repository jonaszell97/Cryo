
import CloudKit
@testable import Cryo

final class MockCloudKitAdaptor {
    /// The database to store to.
    var database: [CKRecord.ID: CKRecord]
    
    /// Cache of schema data.
    var schemas: [ObjectIdentifier: CryoSchema] = [:]
    
    /// Whether CloudKit is available.
    var isAvailable: Bool
    
    /// Default initializer.
    init() {
        self.database = [:]
        self.isAvailable = true
    }
}

final class MockSelectQuery<Model: CryoModel>: CryoSelectQuery {
    let id: String?
    var whereClauses: [CryoQueryWhereClause]
    let allRecords: [CKRecord]
    
    init(id: String?, allRecords: [CKRecord]) {
        self.id = id
        self.allRecords = allRecords
        self.whereClauses = []
    }
    
    var queryString: String { "" }
    
    func `where`<Value: _AnyCryoColumnValue>(_ columnName: String,
                                             operation: Cryo.CryoComparisonOperator,
                                             value: Value) async throws
        -> MockSelectQuery<Model>
    {
        self.whereClauses.append(.init(columnName: columnName, operation: operation,
                                       value: try .init(value: value)))
        return self
    }
    
    func execute() async throws -> [Model] {
        let schema = await CryoSchemaManager.shared.schema(for: Model.self)
        
        var results = [Model]()
        for record in allRecords {
            if let id {
                guard record.recordID.recordName == id else {
                    continue
                }
            }
            
            var matches = true
            var data = [String: _AnyCryoColumnValue]()
            
            for columnDetails in schema {
                guard
                    let object = record[columnDetails.columnName],
                    let value = CloudKitAdaptor.decodeValue(from: object, as: columnDetails.type)
                else {
                    continue
                }
                
                for clause in (whereClauses.filter { $0.columnName == columnDetails.columnName }) {
                    guard try CloudKitAdaptor.check(clause: clause, object: value) else {
                        matches = false
                        break
                    }
                }
                
                data[columnDetails.columnName] = value
            }
            
            guard matches else {
                continue
            }
            
            results.append(try Model(from: CryoModelDecoder(data: data)))
        }
        
        return results
    }
}

extension MockCloudKitAdaptor: AnyCloudKitAdaptor {
    public func createTable<Model: CryoModel>(for model: Model.Type) async throws -> NoOpQuery<Model> {
        NoOpQuery(queryString: "", for: model)
    }
    
    public func select<Model: CryoModel>(id: String? = nil, from: Model.Type) async throws -> MockSelectQuery<Model> {
        MockSelectQuery(id: id, allRecords: self.database.values.map { $0 })
    }
    
    /// Delete a record with the given id.
    func delete(recordWithId id: CKRecord.ID) async throws {
        database[id] = nil
    }
    
    func delete(tableName: String) async throws {
    }
    
    /// Save the given record.
    func save(record: CKRecord) async throws {
        database[record.recordID] = record
    }
    
    /// Fetch a record with the given id.
    func fetch(recordWithId id: CKRecord.ID) async throws -> CKRecord? {
        guard let record = database[id] else {
            return nil
        }
        
        // Replace asset URLs with random new ones
        for key in record.allKeys() {
            guard let asset = record[key] as? CKAsset, let url = asset.fileURL else {
                continue
            }
            
            guard let data = try? Data(contentsOf: url) else {
                continue
            }
            
            let newUrl = DocumentAdaptor.sharedLocal.url.appendingPathComponent("\(UUID()).txt")
            do {
                try data.write(to: newUrl)
                record[key] = CKAsset(fileURL: newUrl)
            }
            catch {
            }
        }
        
        return record
    }
    
    /// Fetch a record with the given id.
    func fetchAllBatched(tableName: String, predicate: NSPredicate, receiveBatch: ([CKRecord]) throws -> Bool) async throws {
        _ = try receiveBatch(database.values.filter { $0.recordType == tableName })
    }
    
    func removeAll() async throws {
        database.removeAll()
    }
}

extension MockCloudKitAdaptor {
    public func ensureAvailability() async throws {
        guard !isAvailable else {
            return
        }
        
        throw CryoError.backendNotAvailable
    }
    
    public func observeAvailabilityChanges(_ callback: @escaping (Bool) -> Void) {
        
    }
}
