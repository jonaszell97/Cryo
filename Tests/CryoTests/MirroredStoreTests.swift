
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

final class MirroredDatabaseStoreTests: XCTestCase {
    static func createMirroredStore(resetState: Bool = true) async throws -> (MirroredDatabaseStore<MockCloudKitAdaptor>, MockCloudKitAdaptor) {
        if resetState {
            try await DocumentAdaptor.sharedLocal.removeAll()
        }
        
        let databaseAdaptor = MockCloudKitAdaptor()
        databaseAdaptor.isAvailable = true
        
        let cryoConfig = CryoConfig { print("[\($0)] \($1)") }
        let config = MirroredDatabaseStoreConfig(identifier: "Test", config: cryoConfig)
        
        let store = try MirroredDatabaseStore(config: config, mainAdaptor: databaseAdaptor)
        try await store.mirrorAdaptor.createTable(for: TestModel.self)
        try await store.mirrorAdaptor.createTable(for: TestModel2.self)
        
        try await store.executeQueuedOperations()
        
        return (store, databaseAdaptor)
    }
    
    static func setAvailability(of store: MirroredDatabaseStore<MockCloudKitAdaptor>, to available: Bool) async throws {
        store.mainAdaptor.isAvailable = available
        
        guard available else {
            return
        }
        
        try await store.executeQueuedOperations()
    }
    
    func testEnabledMirroring() async throws {
        let (store, _) = try await Self.createMirroredStore()
        
        let value = TestModel(x: 123, y: "Hello there")
        let value2 = TestModel(x: 3291, y: "Hello therexxx")
        let value3 = TestModel2(x: 931.32, y: 141)
        
        // Store first value
        do {
            let key = CryoNamedKey(id: "test-123", for: TestModel.self)
            try await store.persist(value, for: key)
            
            let loadedValue = try await store.load(with: key)
            XCTAssertEqual(value, loadedValue)
            
            let key2 = CryoNamedKey(id: "testModel2", for: TestModel2.self)
            try await store.persist(value3, for: key2)
            
            let loadedValue2 = try await store.load(with: key2)
            XCTAssertEqual(value3, loadedValue2)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
        
        // Store second value
        do {
            try await store.persist(value2, for: CryoNamedKey(id: "test-1234", for: TestModel.self))
            
            let allValues = try await store.loadAll(of: TestModel.self)
            XCTAssertNotNil(allValues)
            XCTAssertEqual(Set(allValues!), Set([value, value2]))
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testDisabledMirroring() async throws {
        let (store, _) = try await Self.createMirroredStore()
        
        let value = TestModel(x: 123, y: "Hello there")
        let value2 = TestModel(x: 3291, y: "Hello therexxx")
        
        // Disable the cloud store
        try await Self.setAvailability(of: store, to: false)
        
        do {
            let key = CryoNamedKey(id: "test-123", for: TestModel.self)
            try await store.persist(value, for: key)
            
            let loadedValue = try await store.load(with: key)
            XCTAssertEqual(value, loadedValue)
            
            try await store.persist(value2, for: CryoNamedKey(id: "test-1234", for: TestModel.self))
            
            let allValues = try await store.loadAll(of: TestModel.self)
            XCTAssertNotNil(allValues)
            XCTAssertEqual(Set(allValues!), Set([value, value2]))
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testReenabledMirroring() async throws {
        let (store, _) = try await Self.createMirroredStore()
        
        let value = TestModel(x: 123, y: "Hello there")
        let value2 = TestModel(x: 3291, y: "Hello therexxx")
        let value3 = TestModel2(x: 931.32, y: 141)
        let value4 = TestModel2(x: 74738.1234, y: 8431)
        
        // Disable the cloud store
        try await Self.setAvailability(of: store, to: false)
        
        do {
            let key = CryoNamedKey(id: "test-123", for: TestModel.self)
            try await store.persist(value, for: key)
            
            var loadedValue = try await store.mainAdaptor.load(with: key)
            XCTAssertEqual(nil, loadedValue)
            
            let key2 = CryoNamedKey(id: "testModel2_1", for: TestModel2.self)
            try await store.persist(value3, for: key2)
            
            // Reenable the store.
            try await Self.setAvailability(of: store, to: true)
            
            loadedValue = try await store.mainAdaptor.load(with: key)
            XCTAssertEqual(value, loadedValue)
            
            try await store.persist(value2, for: CryoNamedKey(id: "test-1234", for: TestModel.self))
            
            let allValues = try await store.loadAll(of: TestModel.self)
            XCTAssertNotNil(allValues)
            XCTAssertEqual(Set(allValues!), Set([value, value2]))
            
            let key3 = CryoNamedKey(id: "testModel2_2", for: TestModel2.self)
            try await store.persist(value4, for: key3)
            
            let allValues2 = try await store.loadAll(of: TestModel2.self)
            XCTAssertNotNil(allValues2)
            XCTAssertEqual(Set(allValues2!), Set([value3, value4]))
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testMirroredModifications() async throws {
        let (store, _) = try await Self.createMirroredStore()
        do {
            let value = TestModel(x: 123, y: "Hello there")
            
            // Save a value
            let key = CryoNamedKey(id: "testModel1", for: TestModel.self)
            try await store.persist(value, for: key)
            
            // Disable the cloud store
            try await Self.setAvailability(of: store, to: false)
            
            // Load & modify the value
            var loadedValue = try await store.load(with: key)
            XCTAssertEqual(loadedValue, value)
            
            loadedValue!.x = 3847
            try await store.persist(loadedValue, for: key)
            
            // Ensure changes are (only) saved locally
            loadedValue = try await store.load(with: key)
            XCTAssertNotEqual(loadedValue, value)
            XCTAssertEqual(loadedValue!.x, 3847)
            XCTAssertEqual(loadedValue!.y, value.y)
            
            loadedValue = try await store.mainAdaptor.load(with: key)
            XCTAssertEqual(loadedValue, value)
            
            // Reenable the store
            try await Self.setAvailability(of: store, to: true)
            
            // Ensure changes are propagated
            loadedValue = try await store.load(with: key)
            XCTAssertNotEqual(loadedValue, value)
            XCTAssertEqual(loadedValue!.x, 3847)
            XCTAssertEqual(loadedValue!.y, value.y)
            
            loadedValue = try await store.mainAdaptor.load(with: key)
            XCTAssertNotEqual(loadedValue, value)
            XCTAssertEqual(loadedValue!.x, 3847)
            XCTAssertEqual(loadedValue!.y, value.y)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testMirroredModificationsWithRelaunch() async throws {
        var (store, _) = try await Self.createMirroredStore()
        do {
            let value = TestModel(x: 123, y: "Hello there")
            
            // Save a value
            let key = CryoNamedKey(id: "testModel1", for: TestModel.self)
            try await store.persist(value, for: key)
            
            // Disable the cloud store
            try await Self.setAvailability(of: store, to: false)
            
            // Load & modify the value
            var loadedValue = try await store.load(with: key)
            XCTAssertEqual(loadedValue, value)
            
            loadedValue!.x = 3847
            try await store.persist(loadedValue, for: key)
            
            // Ensure changes are (only) saved locally
            loadedValue = try await store.load(with: key)
            XCTAssertNotEqual(loadedValue, value)
            XCTAssertEqual(loadedValue!.x, 3847)
            XCTAssertEqual(loadedValue!.y, value.y)
            
            loadedValue = try await store.mainAdaptor.load(with: key)
            XCTAssertEqual(loadedValue, value)
            
            // Reload the store
            (store, _) = try await Self.createMirroredStore(resetState: false)
            
            // Reenable the store
            try await Self.setAvailability(of: store, to: true)
            
            // Ensure changes are propagated
            loadedValue = try await store.load(with: key)
            XCTAssertNotEqual(loadedValue, value)
            XCTAssertEqual(loadedValue!.x, 3847)
            XCTAssertEqual(loadedValue!.y, value.y)
            
            loadedValue = try await store.mainAdaptor.load(with: key)
            XCTAssertNotEqual(loadedValue, value)
            XCTAssertEqual(loadedValue!.x, 3847)
            XCTAssertEqual(loadedValue!.y, value.y)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
}
