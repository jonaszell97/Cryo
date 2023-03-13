
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
    
    func testCreateTableQuery() throws {
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!)
        let query = try adaptor.createTableQuery(for: TestModel.self)
        
        XCTAssertEqual(query, """
CREATE TABLE IF NOT EXISTS TestModel(
    _cryo_key TEXT NOT NULL UNIQUE,
    x INTEGER,
    y REAL,
    z INTEGER
);
""");
    }
    
    func testCreateInsertQuery() throws {
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!)
        let value = TestModel(x: 123, y: "Hello there", z: .a)
        
        XCTAssertEqual(try adaptor.createInsertQuery(for: value), """
INSERT INTO TestModel(_cryo_key,x,y,z) VALUES (?,?,?,?);
""")
    }
    
    
    func testDatabasePersistence() async throws {
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!)
        
        let value = TestModel(x: 123, y: "Hello there", z: .a)
        let value2 = TestModel(x: 3291, y: "Hello therexxx", z: .c)
        
        XCTAssertEqual(TestModel.schema.map { $0.columnName }, ["x", "y", "z"])
        
        do {
            let key = AnyKey(id: "test-123", for: TestModel.self)
            try await adaptor.persist(value, for: key)
            
            let loadedValue = try await adaptor.load(with: key)
            XCTAssertEqual(value, loadedValue)
            
            try await adaptor.persist(value2, for: AnyKey(id: "test-1234", for: TestModel.self))
            
            let allValues = try await adaptor.loadAll(with: AnyKey<TestModel>.self)
            XCTAssertNotNil(allValues)
            XCTAssertEqual(Set(allValues!), Set([value, value2]))
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
}
