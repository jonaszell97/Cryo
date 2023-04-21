import XCTest
@testable import Cryo

fileprivate struct TestModel: CryoModel {
    @CryoColumn var id: String = UUID().uuidString
    @CryoColumn var x: Int16 = 0
    @CryoColumn var y: String = ""
    
    static func random(id: String = UUID().uuidString) -> Self {
        .init(id: id, x: .random(in: Int16.min...Int16.max), y: String((0..<10).map { _ in
            "abcdefghijklmnopqrstuvwxyz0123456789".randomElement()!
        }))
    }
}

extension TestModel: Hashable {
    static func ==(lhs: TestModel, rhs: TestModel) -> Bool {
        lhs.x == rhs.x && lhs.y == rhs.y
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(x)
        hasher.combine(y)
    }
}

final class SynchronizedStoreTests: XCTestCase {
    private typealias StoreType = SynchronizedStoreImpl<MockCloudKitAdaptor>
    
    override func setUp() async throws {
        try await UserDefaultsAdaptor.shared.removeAll()
        try await DocumentAdaptor.sharedLocal.removeAll()
    }
    
    private func createStore(identifier: String, deviceIdentifier: String, backend: MockCloudKitAdaptor? = nil) async throws -> StoreType {
        let config = SynchronizedStoreConfig(storeIdentifier: identifier,
                                             localDatabaseUrl: DocumentAdaptor.sharedLocal.url.appendingPathComponent("\(UUID().uuidString).db"),
                                             containerIdentifier: "",
                                             managedModels: [TestModel.self],
                                             cryoConfig: CryoConfig { print("[\($0)] \($1)") })
        
        let backend = backend ?? MockCloudKitAdaptor()
        try await backend.createTable(for: TestModel.self).execute()
        
        return try await StoreType(config: config, backend: backend, deviceIdentifier: deviceIdentifier)
    }
    
    private func persistAndLoadTest(_ value: TestModel, to store: StoreType) async throws {
        try await store.insert(value).execute()
        
        var loadedValue = try await store.select(id: value.id, from: TestModel.self).execute()
        XCTAssertEqual(loadedValue.first, value)
        
        try await store.delete(from: TestModel.self)
            .where("x", equals: value.x)
            .execute()
        
        loadedValue = try await store.select(id: value.id, from: TestModel.self).execute()
        XCTAssertEqual(loadedValue.count, 0)
    }
    
    func testSingleDeviceFunctionality() async throws {
        let store = try await createStore(identifier: "TestStore", deviceIdentifier: "iPhone13,1")
        do {
            for _ in 0..<100 {
                let model = TestModel.random()
                try await self.persistAndLoadTest(model, to: store)
            }
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testInsertSynchronisation() async throws {
        let phoneStore = try await createStore(identifier: "TestStore", deviceIdentifier: "iPhone13,1")
        let tabletStore = try await createStore(identifier: "TestStore", deviceIdentifier: "iPad14,2",
                                                backend: phoneStore.operationsStore)
        
        var values = [TestModel]()
        
        // Create records on one device
        for i in 0..<100 {
            let id = "\(i)"
            let model = TestModel.random(id: id)
            values.append(model)
            
            try await phoneStore.insert(model)
                .execute()
        }
        
        // Retrieve records on other device
        for i in 0..<100 {
            let id = "\(i)"
            let record = try await tabletStore.select(id: id, from: TestModel.self)
                .execute().first
            
            XCTAssertEqual(record, values[i])
        }
    }
    
    func testDeleteSynchronisation() async throws {
        let phoneStore = try await createStore(identifier: "TestStore", deviceIdentifier: "iPhone13,1")
        let tabletStore = try await createStore(identifier: "TestStore", deviceIdentifier: "iPad14,2",
                                                backend: phoneStore.operationsStore)
        
        var values = [TestModel]()
        
        // Create records on one device
        for i in 0..<10 {
            let id = "\(i)"
            let model = TestModel.random(id: id)
            values.append(model)
            
            try await phoneStore.insert(model)
                .execute()
        }
        
        // Delete records on other device
        for i in 0..<10 {
            let id = "\(i)"
            try await tabletStore.delete(id: id, from: TestModel.self)
                .execute()
        }
        
        // Load on original device
        for i in 0..<10 {
            let id = "\(i)"
            let record = try await phoneStore.select(id: id, from: TestModel.self)
                .execute().first
            
            XCTAssertNil(record)
        }
    }
    
    func testUpdateSynchronisation() async throws {
        let phoneStore = try await createStore(identifier: "TestStore", deviceIdentifier: "iPhone13,1")
        let tabletStore = try await createStore(identifier: "TestStore", deviceIdentifier: "iPad14,2",
                                                backend: phoneStore.operationsStore)
        
        var values = [TestModel]()
        
        // Create records on one device
        for i in 0..<10 {
            let id = "\(i)"
            let model = TestModel.random(id: id)
            values.append(model)
            
            try await phoneStore.insert(model)
                .execute()
        }
        
        // Modify records on other device
        for i in 0..<10 {
            let id = "\(i)"
            let record = try await tabletStore.select(id: id, from: TestModel.self)
                .execute().first
            
            XCTAssertNotNil(record)
            
            try await tabletStore.update(id: id, from: TestModel.self)
                .set("x", to: record!.x + 1)
                .execute()
        }
        
        // Load on original device
        for i in 0..<10 {
            let id = "\(i)"
            let record = try await phoneStore.select(id: id, from: TestModel.self)
                .execute().first
            
            XCTAssertEqual(record?.x, values[i].x + 1)
        }
    }
    
    func testStoreInitialization() async throws {
        let phoneStore = try await createStore(identifier: "TestStore", deviceIdentifier: "iPhone13,1")
        
        var values = [TestModel]()
        
        // Create records on one device
        for i in 0..<10 {
            let id = "\(i)"
            let model = TestModel.random(id: id)
            values.append(model)
            
            try await phoneStore.insert(model)
                .execute()
        }
        
        // Initialize other device
        let tabletStore = try await createStore(identifier: "TestStore", deviceIdentifier: "iPad14,2",
                                                backend: phoneStore.operationsStore)
        
        
        // Retrieve records on other device
        for i in 0..<10 {
            let id = "\(i)"
            let record = try await tabletStore.select(id: id, from: TestModel.self)
                .execute().first
            
            XCTAssertEqual(record, values[i])
        }
    }
}
