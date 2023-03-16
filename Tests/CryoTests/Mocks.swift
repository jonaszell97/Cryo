
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

extension MockCloudKitAdaptor: AnyCloudKitAdaptor {
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
