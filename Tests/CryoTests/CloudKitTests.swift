
import XCTest
@testable import Cryo

fileprivate enum TestEnum: Int, CryoColumnIntValue, Hashable {
    case zero = 0
    case a = 300, b = 400, c = 500
}

fileprivate struct TestModel: CryoModel {
    @CryoColumn var id: String = UUID().uuidString
    @CryoColumn var x: Int16 = 0
    @CryoColumn var y: String = ""
    @CryoColumn var z: TestEnum = .c
    
    @CryoAsset var w: URL
    
    @CryoColumn var a: [TestEnum] = [.a, .b, .c]
    @CryoColumn var b: Int? = nil
    @CryoColumn var c: [String: Int] = ["A": 1, "B": 2]
}

extension TestModel: Hashable {
    static func ==(lhs: TestModel, rhs: TestModel) -> Bool {
        guard lhs.x == rhs.x && lhs.y == rhs.y && lhs.z == rhs.z else {
            return false
        }
        guard lhs.a == rhs.a && lhs.b == rhs.b && lhs.c == rhs.c else {
            return false
        }
        
        guard let data1 = try? Data(contentsOf: lhs.w), let data2 = try? Data(contentsOf: rhs.w) else {
            return false
        }
        
        return data1 == data2
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(x)
        hasher.combine(y)
        hasher.combine(z)
        hasher.combine(a)
        hasher.combine(b)
        hasher.combine(c)
        
        if let data = try? Data(contentsOf: w) {
            hasher.combine(data)
        }
    }
}

final class CryoDatabaseTests: XCTestCase {
    struct AnyKey<Value: CryoModel>: CryoKey {
        let id: String
        
        init(id: String, for: Value.Type) {
            self.id = id
        }
    }
    
    func testDatabasePersistence() async {
        let adaptor = MockCloudKitAdaptor()
        
        let assetUrl = DocumentAdaptor.sharedLocal.url.appendingPathComponent("testAsset.txt")
        do {
            try? FileManager.default.removeItem(at: assetUrl)
            try "Hello, World!".write(to: assetUrl, atomically: true, encoding: .utf8)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
        
        let value = TestModel(x: 123, y: "Hello there", z: .a, w: assetUrl)
        let value2 = TestModel(x: 3291, y: "Hello therexxx", z: .c, w: assetUrl)
        
        XCTAssertEqual(TestModel.schema.columns.map { $0.columnName }, ["id", "x", "y", "z", "w", "a", "b", "c"])
        
        do {
            try await adaptor.createTable(for: TestModel.self).execute()
            
            _ = try await adaptor.insert(value).execute()
            
            let loadedValue = try await adaptor.select(id: value.id, from: TestModel.self).execute().first
            XCTAssertEqual(value, loadedValue)
            
            _ = try await adaptor.insert(value2).execute()
            
            let allValues = try await adaptor.select(from: TestModel.self).execute()
            XCTAssertNotNil(allValues)
            XCTAssertEqual(Set(allValues), Set([value, value2]))
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
}
