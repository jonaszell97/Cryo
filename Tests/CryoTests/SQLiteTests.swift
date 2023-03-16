
import XCTest
@testable import Cryo

fileprivate enum TestEnum: Int, CryoColumnIntValue, Hashable {
    case zero = 0
    case a = 300, b = 400, c = 500
}

fileprivate struct TestModel: CryoModel {
    @CryoColumn var x: Int16 = 0
    @CryoColumn var y: String = ""
    @CryoColumn var z: TestEnum = .c
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
    
    func testCreateInsertQuery() async throws {
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!)
        let value = TestModel(x: 123, y: "Hello there", z: .a)
        let query = try await adaptor.createInsertQuery(for: value)
        
        XCTAssertEqual(query, """
INSERT OR REPLACE INTO TestModel(_cryo_key,_cryo_created,_cryo_modified,x,y,z) VALUES (?,?,?,?,?,?);
""")
    }
    
    
    func testDatabasePersistence() async throws {
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!, config: CryoConfig { print("[\($0)] \($1)") })
        
        let value = TestModel(x: 123, y: "Hello there", z: .a)
        let value2 = TestModel(x: 3291, y: "Hello therexxx", z: .c)
        
        XCTAssertEqual(TestModel.schema.map { $0.columnName }, ["x", "y", "z"])
        
        do {
            let key = AnyKey(id: "test-123", for: TestModel.self)
            try await adaptor.persist(value, for: key)
            
            let loadedValue = try await adaptor.load(with: key)
            XCTAssertEqual(value, loadedValue)
            
            try await adaptor.persist(value2, for: AnyKey(id: "test-1234", for: TestModel.self))
            
            let allValues = try await adaptor.loadAll(of: TestModel.self)
            XCTAssertNotNil(allValues)
            XCTAssertEqual(Set(allValues!), Set([value, value2]))
            
            try await adaptor.removeAll(of: TestModel.self)
            
            let allValues2 = try await adaptor.loadAll(of: TestModel.self)
            XCTAssertEqual(allValues2?.count, 0)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testSelectQueries() async throws {
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!, config: CryoConfig { print("[\($0)] \($1)") })
        
        do {
            let value = TestModel(x: 123, y: "Hello there", z: .a)
            let key = AnyKey(id: "test-123", for: TestModel.self)
            try await adaptor.persist(value, for: key)
            
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
}
