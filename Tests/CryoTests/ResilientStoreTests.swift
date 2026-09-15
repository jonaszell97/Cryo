
import XCTest
@testable import Cryo

fileprivate struct TestModel: CryoModel {
    @CryoColumn var id: String = UUID().uuidString
    @CryoColumn var x: Int16
    @CryoColumn var y: String
}

extension TestModel: Hashable {
    static func ==(lhs: Self, rhs: Self) -> Bool {
        return (
            lhs.x == rhs.x
            && lhs.y == rhs.y
        )
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(x)
        hasher.combine(y)
    }
}

fileprivate struct TestModel2: CryoModel {
    @CryoColumn var id: String = UUID().uuidString
    @CryoColumn var x: Double
    @CryoColumn var y: Int
}

extension TestModel2: Hashable {
    static func ==(lhs: Self, rhs: Self) -> Bool {
        return (
            lhs.x == rhs.x
            && lhs.y == rhs.y
        )
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(x)
        hasher.combine(y)
    }
}

final class ResilientStoreTests: CryoTestCase {
    typealias StoreType = ResilientStoreImpl<CloudKitAdaptor>
    
    func createResilientStore(database: CloudKitAdaptor? = nil, resetState: Bool = true) async throws -> StoreType {
        if resetState { try await environment.documents.removeAll() }
        let databaseAdaptor = database ?? environment.cloud
        environment.database.isOnline = true

        try await databaseAdaptor.createTable(for: TestModel.self).execute()
        try await databaseAdaptor.createTable(for: TestModel2.self).execute()
        
        let cryoConfig = environment.config
        let config = ResilientCloudKitStoreConfig(identifier: "TestStore_resilient", maximumNumberOfRetries: 5, cryoConfig: cryoConfig, queueStore: environment.documents)
        
        return try await StoreType(store: databaseAdaptor, config: config)
    }
    
    func setAvailability(of store: StoreType, to available: Bool) async throws {
        environment.database.isOnline = available
        
        guard available else {
            return
        }
        
        try await store.executeFailedOperations()
    }
    
    private func readRemote<Model: CryoModel>(_ store: StoreType, id: String? = nil, from model: Model.Type) async throws -> [Model] {
        let wasOnline = environment.database.isOnline
        environment.database.isOnline = true
        defer { environment.database.isOnline = wasOnline }
        // Query by predicate so a missing record produces an empty page. Direct ID
        // lookup currently throws unknownItem; parity is scheduled for phase 4.
        let query = try await store.select(from: model)
        if let id { _ = try query.where("id", equals: id) }
        return try await query.execute()
    }

    func testEnabledMirroring() async throws {
        let store = try await createResilientStore()
        
        let value = TestModel(x: 123, y: "Hello there")
        let value2 = TestModel(x: 3291, y: "Hello therexxx")
        let value3 = TestModel2(x: 931.32, y: 141)
        
        // Store first value
        do {
            try await store.insert(value, replace: false).execute()
            
            let loadedValue = try await readRemote(store, id: value.id, from: TestModel.self).first
            XCTAssertEqual(value, loadedValue)
            
            try await store.insert(value3, replace: false).execute()
            
            let loadedValue2 = try await readRemote(store, id: value3.id, from: TestModel2.self).first
            XCTAssertEqual(value3, loadedValue2)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
        
        // Store second value
        do {
            try await store.insert(value2).execute()
            
            let allValues = try await readRemote(store, from: TestModel.self)
            XCTAssertNotNil(allValues)
            XCTAssertEqual(Set(allValues), Set([value, value2]))
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testDisabledMirroring() async throws {
        let store = try await createResilientStore()
        
        let value = TestModel(x: 123, y: "Hello there")
        let value2 = TestModel(x: 3291, y: "Hello therexxx")
        
        // Disable the cloud store
        try await setAvailability(of: store, to: false)
        
        do {
            try await store.insert(value, replace: false).execute()
            try await store.insert(value2).execute()
            
            let allValues = try await readRemote(store, from: TestModel.self)
            XCTAssertEqual(allValues.count, 0)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testReenabledMirroring() async throws {
        let store = try await createResilientStore()
        
        let value = TestModel(x: 123, y: "Hello there")
        let value2 = TestModel(x: 3291, y: "Hello therexxx")
        
        // Disable the cloud store
        try await setAvailability(of: store, to: false)
        
        do {
            try await store.insert(value, replace: false).execute()
            try await store.insert(value2).execute()
            
            var allValues = try await readRemote(store, from: TestModel.self)
            XCTAssertEqual(allValues.count, 0)
            
            // Reenable store
            try await setAvailability(of: store, to: true)
            
            allValues = try await readRemote(store, from: TestModel.self)
            XCTAssertNotNil(allValues)
            XCTAssertEqual(Set(allValues), Set([value, value2]))
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testUpdatePropagation() async throws {
        let store = try await createResilientStore()
        do {
            let value = TestModel(x: 123, y: "Hello there")
            
            // Save a value
            try await store.insert(value).execute()
            
            // Disable the cloud store
            try await setAvailability(of: store, to: false)
            
            // Modify the value
            try await store.update(id: value.id, from: TestModel.self)
                .set("x", to: 3847)
                .execute()
            
            // Changes should not be reflected locally
            var loadedValue = try await readRemote(store, id: value.id, from: TestModel.self).first
            XCTAssertEqual(loadedValue, value)
            
            // Reenable store
            try await setAvailability(of: store, to: true)
            
            // Ensure changes are propagated
            loadedValue = try await readRemote(store, id: value.id, from: TestModel.self).first
            XCTAssertEqual(loadedValue?.x, 3847)
            XCTAssertEqual(loadedValue?.y, value.y)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testUpdatePropagationWithRelaunch() async throws {
        let database = environment.cloud
        let value = TestModel(x: 123, y: "Hello there")
        do {
            let store = try await createResilientStore(database: database)
            
            // Save a value
            try await store.insert(value).execute()
            
            // Disable the cloud store
            try await setAvailability(of: store, to: false)
            
            // Modify the value
            try await store.update(id: value.id, from: TestModel.self)
                .set("x", to: 3847)
                .execute()
            
            // Changes should not be reflected locally
            let loadedValue = try await readRemote(store, id: value.id, from: TestModel.self).first
            XCTAssertEqual(loadedValue, value)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
        
        do {
            // Recreate the store
            let store = try await createResilientStore(database: database, resetState: false)
            
            // Ensure changes are propagated
            let loadedValue = try await readRemote(store, id: value.id, from: TestModel.self).first
            XCTAssertEqual(loadedValue?.x, 3847)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testDeletePropagation() async throws {
        let store = try await createResilientStore()
        do {
            let value = TestModel(x: 123, y: "Hello there")
            
            // Save a value
            try await store.insert(value).execute()
            
            // Disable the cloud store
            try await setAvailability(of: store, to: false)
            
            // Delete the value
            try await store.delete(id: value.id, from: TestModel.self)
                .execute()
            
            // Changes should not be reflected locally
            var loadedValue = try await readRemote(store, id: value.id, from: TestModel.self).first
            XCTAssertEqual(loadedValue, value)
            
            // Reenable store
            try await setAvailability(of: store, to: true)
            
            // Ensure changes are propagated
            loadedValue = try await readRemote(store, id: value.id, from: TestModel.self).first
            XCTAssertNil(loadedValue)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testDeletePropagationWithRelaunch() async throws {
        let database = environment.cloud
        let value = TestModel(x: 123, y: "Hello there")
        do {
            let store = try await createResilientStore(database: database)
            
            // Save a value
            try await store.insert(value).execute()
            
            // Disable the cloud store
            try await setAvailability(of: store, to: false)
            
            // Delete the value
            try await store.delete(id: value.id, from: TestModel.self)
                .execute()
            
            // Changes should not be reflected locally
            let loadedValue = try await readRemote(store, id: value.id, from: TestModel.self).first
            XCTAssertEqual(loadedValue, value)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
        
        do {
            // Recreate the store
            let store = try await createResilientStore(database: database, resetState: false)
            
            // Ensure changes are propagated
            let loadedValue = try await readRemote(store, id: value.id, from: TestModel.self).first
            XCTAssertNil(loadedValue)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
}
