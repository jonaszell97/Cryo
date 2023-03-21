
import XCTest
@testable import Cryo

fileprivate struct TestModel: CryoModel {
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

final class ResilientStoreTests: XCTestCase {
    typealias StoreType = ResilientStoreImpl<MockCloudKitAdaptor>
    
    static func createResilientStore(resetState: Bool = true) async throws -> StoreType {
        if resetState {
            try await DocumentAdaptor.sharedLocal.removeAll()
        }
        
        let databaseAdaptor = MockCloudKitAdaptor()
        databaseAdaptor.isAvailable = true
        
        let cryoConfig = CryoConfig { print("[\($0)] \($1)") }
        let config = ResilientCloudKitStoreConfig(identifier: "TestStore_resilient", maximumNumberOfRetries: 5, cryoConfig: cryoConfig)
        
        return await StoreType(store: databaseAdaptor, config: config)
    }
    
    static func setAvailability(of store: StoreType, to available: Bool) async {
        store.store.isAvailable = available
        
        guard available else {
            return
        }
        
        await store.executeFailedOperations()
    }
    
    func testEnabledMirroring() async throws {
        let store = try await Self.createResilientStore()
        
        let value = TestModel(x: 123, y: "Hello there")
        let value2 = TestModel(x: 3291, y: "Hello therexxx")
        let value3 = TestModel2(x: 931.32, y: 141)
        
        // Store first value
        do {
            let key = "test-123"
            try await store.insert(id: key, value, replace: false).execute()
            
            let loadedValue = try await store.select(id: key, from: TestModel.self).execute().first
            XCTAssertEqual(value, loadedValue)
            
            let key2 = "testModel2"
            try await store.insert(id: key2, value3, replace: false).execute()
            
            let loadedValue2 = try await store.select(id: key2, from: TestModel2.self).execute().first
            XCTAssertEqual(value3, loadedValue2)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
        
        // Store second value
        do {
            try await store.insert(id: "test-1234", value2).execute()
            
            let allValues = try await store.select(from: TestModel.self).execute()
            XCTAssertNotNil(allValues)
            XCTAssertEqual(Set(allValues), Set([value, value2]))
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testDisabledMirroring() async throws {
        let store = try await Self.createResilientStore()
        
        let value = TestModel(x: 123, y: "Hello there")
        let value2 = TestModel(x: 3291, y: "Hello therexxx")
        
        // Disable the cloud store
        await Self.setAvailability(of: store, to: false)
        
//        do {
//            let key = CryoNamedKey(id: "test-123", for: TestModel.self)
//            try await store.persist(value, for: key)
//
//            let loadedValue = try await store.load(with: key)
//            XCTAssertEqual(value, loadedValue)
//
//            try await store.persist(value2, for: CryoNamedKey(id: "test-1234", for: TestModel.self))
//
//            let allValues = try await store.loadAll(of: TestModel.self)
//            XCTAssertNotNil(allValues)
//            XCTAssertEqual(Set(allValues!), Set([value, value2]))
//        }
//        catch {
//            XCTAssert(false, error.localizedDescription)
//        }
    }
    
    func testReenabledMirroring() async throws {
        let store = try await Self.createResilientStore()
        
        let value = TestModel(x: 123, y: "Hello there")
        let value2 = TestModel(x: 3291, y: "Hello therexxx")
        let value3 = TestModel2(x: 931.32, y: 141)
        let value4 = TestModel2(x: 74738.1234, y: 8431)
        
        // Disable the cloud store
        await Self.setAvailability(of: store, to: false)
        
        do {
//            let key = CryoNamedKey(id: "test-123", for: TestModel.self)
//            try await store.persist(value, for: key)
//
//            var loadedValue = try await store.mainAdaptor.load(with: key)
//            XCTAssertEqual(nil, loadedValue)
//
//            let key2 = CryoNamedKey(id: "testModel2_1", for: TestModel2.self)
//            try await store.persist(value3, for: key2)
//
//            // Reenable the store.
//            try await Self.setAvailability(of: store, to: true)
//
//            loadedValue = try await store.mainAdaptor.load(with: key)
//            XCTAssertEqual(value, loadedValue)
//
//            try await store.persist(value2, for: CryoNamedKey(id: "test-1234", for: TestModel.self))
//
//            let allValues = try await store.loadAll(of: TestModel.self)
//            XCTAssertNotNil(allValues)
//            XCTAssertEqual(Set(allValues!), Set([value, value2]))
//
//            let key3 = CryoNamedKey(id: "testModel2_2", for: TestModel2.self)
//            try await store.persist(value4, for: key3)
//
//            let allValues2 = try await store.loadAll(of: TestModel2.self)
//            XCTAssertNotNil(allValues2)
//            XCTAssertEqual(Set(allValues2!), Set([value3, value4]))
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testMirroredModifications() async throws {
        let store = try await Self.createResilientStore()
        do {
            let value = TestModel(x: 123, y: "Hello there")
            
            // Save a value
//            let key = CryoNamedKey(id: "testModel1", for: TestModel.self)
//            try await store.persist(value, for: key)
//
//            // Disable the cloud store
//            try await Self.setAvailability(of: store, to: false)
//
//            // Load & modify the value
//            var loadedValue = try await store.load(with: key)
//            XCTAssertEqual(loadedValue, value)
//
//            loadedValue!.x = 3847
//            try await store.persist(loadedValue, for: key)
//
//            // Ensure changes are (only) saved locally
//            loadedValue = try await store.load(with: key)
//            XCTAssertNotEqual(loadedValue, value)
//            XCTAssertEqual(loadedValue!.x, 3847)
//            XCTAssertEqual(loadedValue!.y, value.y)
//
//            loadedValue = try await store.mainAdaptor.load(with: key)
//            XCTAssertEqual(loadedValue, value)
//
//            // Reenable the store
//            try await Self.setAvailability(of: store, to: true)
//
//            // Ensure changes are propagated
//            loadedValue = try await store.load(with: key)
//            XCTAssertNotEqual(loadedValue, value)
//            XCTAssertEqual(loadedValue!.x, 3847)
//            XCTAssertEqual(loadedValue!.y, value.y)
//
//            loadedValue = try await store.mainAdaptor.load(with: key)
//            XCTAssertNotEqual(loadedValue, value)
//            XCTAssertEqual(loadedValue!.x, 3847)
//            XCTAssertEqual(loadedValue!.y, value.y)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testMirroredModificationsWithRelaunch() async throws {
        var store = try await Self.createResilientStore()
        do {
            let value = TestModel(x: 123, y: "Hello there")
            
            // Save a value
//            let key = CryoNamedKey(id: "testModel1", for: TestModel.self)
//            try await store.persist(value, for: key)
//
//            // Disable the cloud store
//            try await Self.setAvailability(of: store, to: false)
//
//            // Load & modify the value
//            var loadedValue = try await store.load(with: key)
//            XCTAssertEqual(loadedValue, value)
//
//            loadedValue!.x = 3847
//            try await store.persist(loadedValue, for: key)
//
//            // Ensure changes are (only) saved locally
//            loadedValue = try await store.load(with: key)
//            XCTAssertNotEqual(loadedValue, value)
//            XCTAssertEqual(loadedValue!.x, 3847)
//            XCTAssertEqual(loadedValue!.y, value.y)
//
//            loadedValue = try await store.mainAdaptor.load(with: key)
//            XCTAssertEqual(loadedValue, value)
//
//            // Reload the store
//            (store, _) = try await Self.createMirroredStore(resetState: false)
//
//            // Reenable the store
//            try await Self.setAvailability(of: store, to: true)
//
//            // Ensure changes are propagated
//            loadedValue = try await store.load(with: key)
//            XCTAssertNotEqual(loadedValue, value)
//            XCTAssertEqual(loadedValue!.x, 3847)
//            XCTAssertEqual(loadedValue!.y, value.y)
//
//            loadedValue = try await store.mainAdaptor.load(with: key)
//            XCTAssertNotEqual(loadedValue, value)
//            XCTAssertEqual(loadedValue!.x, 3847)
//            XCTAssertEqual(loadedValue!.y, value.y)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
}
