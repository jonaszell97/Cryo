
import XCTest
@testable import Cryo

fileprivate enum TestEnum: Int, CryoColumnIntValue, Hashable, CaseIterable {
    case zero = 0
    case a = 300, b = 400, c = 500
}

fileprivate struct TestModel: CryoModel {
    @CryoColumn var x: Int16 = 0
    @CryoColumn var y: String = ""
    @CryoColumn var z: TestEnum = .c
    
    static func random() -> Self {
        .init(x: .random(in: Int16.min...Int16.max), y: String((0..<10).map { _ in
            "abcdefghijklmnopqrstuvwxyz0123456789".randomElement()!
        }), z: .allCases.randomElement()!)
    }
}

extension TestModel: Hashable {
    static func ==(lhs: TestModel, rhs: TestModel) -> Bool {
        lhs.x == rhs.x && lhs.y == rhs.y && lhs.z == rhs.z
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(x)
        hasher.combine(y)
        hasher.combine(z)
    }
}

final class CryoSQLiteTests: XCTestCase {
    struct AnyKey<Value: CryoModel>: CryoKey {
        let id: String
        
        init(id: String, for: Value.Type) {
            self.id = id
        }
    }
    
    private var databaseUrl: URL? = nil
    
    override func setUp() {
        super.setUp()
        
        self.databaseUrl = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0].appendingPathComponent("_cryo_test.db")
        
        do { try FileManager.default.removeItem(at: self.databaseUrl!) } catch { }
        FileManager.default.createFile(atPath: self.databaseUrl!.absoluteString, contents: nil)
    }
    
    func testCreateTableQuery() async throws {
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!)
        let query = try await adaptor.createTable(for: TestModel.self).queryString
        
        XCTAssertEqual(query, """
CREATE TABLE IF NOT EXISTS TestModel(
    _cryo_key TEXT NOT NULL UNIQUE,
    _cryo_created TEXT NOT NULL,
    _cryo_modified TEXT NOT NULL,
    x INTEGER,
    y REAL,
    z INTEGER
);
""");
    }
    
    private func persistAndLoadTest(_ value: TestModel, to store: SQLiteAdaptor) async throws {
        let id = UUID().uuidString
        try await store.insert(id: id, value).execute()
        
        var loadedValue = try await store.select(id: id, from: TestModel.self).execute()
        XCTAssertEqual(loadedValue.first, value)
        
        try await store.delete(from: TestModel.self)
            .where("x", equals: value.x)
            .execute()
        
        loadedValue = try await store.select(id: id, from: TestModel.self).execute()
        XCTAssertEqual(loadedValue.count, 0)
    }
    
    private func persistAndLoadOperationTest(_ value: TestModel, to store: SQLiteAdaptor) async throws {
        let id = UUID().uuidString
        
        let operation = try await store.insert(id: id, value).operation
        try await store.execute(operation: operation)
        
        var loadedValue = try await store.select(id: id, from: TestModel.self).execute()
        XCTAssertEqual(loadedValue.first, value)
        
        let deleteOperation = try await store.delete(from: TestModel.self)
            .where("x", equals: value.x)
            .operation
        try await store.execute(operation: deleteOperation)
        
        loadedValue = try await store.select(id: id, from: TestModel.self).execute()
        XCTAssertEqual(loadedValue.count, 0)
    }
    
    func testDatabasePersistence() async throws {
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!, config: CryoConfig { print("[\($0)] \($1)") })
        try await adaptor.createTable(for: TestModel.self).execute()
        
        let value = TestModel(x: 123, y: "Hello there", z: .a)
        let value2 = TestModel(x: 3291, y: "Hello therexxx", z: .c)
        
        XCTAssertEqual(TestModel.schema.columns.map { $0.columnName }, ["x", "y", "z"])
        
        do {
            let key = "test-123"
            _ = try await adaptor.insert(id: key, value).execute()
            
            let loadedValue = try await adaptor.select(id: key, from: TestModel.self).execute().first
            XCTAssertEqual(value, loadedValue)
            
            _ = try await adaptor.insert(id: "test-1234", value2).execute()
            
            let allValues = try await adaptor.select(from: TestModel.self).execute()
            XCTAssertNotNil(allValues)
            XCTAssertEqual(Set(allValues), Set([value, value2]))
            
            _ = try await adaptor.delete(id: "test-1234", from: TestModel.self).execute()
            let count = try await adaptor.select(from: TestModel.self).execute().count
            XCTAssertEqual(count, 1)
            
            _ = try await adaptor.delete(from: TestModel.self).execute()
            
            let allValues2 = try await adaptor.select(from: TestModel.self).execute()
            XCTAssertEqual(allValues2.count, 0)
            
            for _ in 0..<100 {
                let model = TestModel.random()
                try await self.persistAndLoadTest(model, to: adaptor)
                try await self.persistAndLoadOperationTest(model, to: adaptor)
            }
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testSelectQueries() async throws {
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!, config: CryoConfig { print("[\($0)] \($1)") })
        try await adaptor.createTable(for: TestModel.self).execute()

        do {
            let value = TestModel(x: 123, y: "Hello there", z: .a)

            let inserted = try await adaptor.insert(id: "test-123", value).execute()
            XCTAssertEqual(inserted, true)

            let result0 = try await adaptor
                .select(from: TestModel.self)
                .execute()

            XCTAssertEqual(result0, [value])

            let result1 = try await adaptor
                .select(from: TestModel.self)
                .where("x", equals: 123)
                .execute()

            XCTAssertEqual(result1, [value])

            let result2 = try await adaptor
                .select(from: TestModel.self)
                .where("x", equals: 123)
                .where("y", equals: "Hmmm")
                .execute()

            XCTAssertEqual(result2.count, 0)

            let result3 = try await adaptor
                .select(from: TestModel.self)
                .where("x", isGreatherThan: 50)
                .where("x", isLessThan: 200)
                .execute()

            XCTAssertEqual(result3, [value])
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testInsertQueries() async throws {
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!, config: CryoConfig { print("[\($0)] \($1)") })
        try await adaptor.createTable(for: TestModel.self).execute()
        
        do {
            let value = TestModel(x: 123, y: "Hello there", z: .a)
            
            let inserted = try await adaptor.insert(id: "test-123", value, replace: true).execute()
            XCTAssertEqual(inserted, true)

            do {
                _ = try await adaptor.insert(id: "test-123", value, replace: false).execute()
                XCTFail("should not be reached")
            }
            catch let e as CryoError {
                if case .duplicateId = e {
                }
                else {
                    XCTFail("error should be duplicateId")
                }
            }
            catch {
                XCTFail("should not be reached")
            }
        }
        catch {
            XCTFail(error.localizedDescription)
        }
    }
    
    func testDeleteQueries() async throws {
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!, config: CryoConfig { print("[\($0)] \($1)") })
        try await adaptor.createTable(for: TestModel.self).execute()
        
        var models = [TestModel]()
        
        for i in 0..<100 {
            let id = "id\(i)"
            let model = TestModel.random()
            models.append(model)
            
            _ = try await adaptor.insert(id: id, model).execute()
        }
        
        let idsToDelete = Set((0..<10).map { _ in (0..<100).randomElement()! })
        for i in idsToDelete {
            let id = "id\(i)"
            _ = try await adaptor.delete(id: id, from: TestModel.self).execute()
        }
        
        for i in 0..<100 {
            let id = "id\(i)"
            let value = try await adaptor.select(id: id, from: TestModel.self).execute()
            
            if idsToDelete.contains(i) {
                XCTAssertEqual(value.count, 0)
            }
            else {
                XCTAssertEqual(value.count, 1)
                XCTAssertEqual(value.first, models[i])
            }
        }
    }
}
