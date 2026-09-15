
import XCTest
@testable import Cryo

final class CryoDatabaseTests: CryoTestCase {
    struct AnyKey<Value: CryoModel>: CryoKey {
        let id: String
        
        init(id: String) {
            self.id = id
        }

        init(id: String, for: Value.Type) {
            self.id = id
        }
    }
    
    func testDatabasePersistence() async {
        let adaptor = environment.cloud
        
        let assetUrl = environment.root.appendingPathComponent("testAsset.txt")
        do {
            try? FileManager.default.removeItem(at: assetUrl)
            try "Hello, World!".write(to: assetUrl, atomically: true, encoding: .utf8)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
        
        let value = CloudKitTestModel(x: 123, y: "Hello there", z: .a, w: assetUrl)
        let value2 = CloudKitTestModel(x: 3291, y: "Hello therexxx", z: .c, w: assetUrl)
        
        XCTAssertEqual(try CloudKitTestModel.schema.columns.map { $0.columnName }, ["id", "x", "y", "z", "w", "a", "b", "c"])
        
        do {
            try await adaptor.createTable(for: CloudKitTestModel.self).execute()
            
            _ = try await adaptor.insert(value).execute()
            
            let loadedValue = try await adaptor.select(id: value.id, from: CloudKitTestModel.self).execute().first
            XCTAssertEqual(value, loadedValue)
            
            _ = try await adaptor.insert(value2).execute()
            
            let allValues = try await adaptor.select(from: CloudKitTestModel.self).execute()
            XCTAssertNotNil(allValues)
            XCTAssertEqual(Set(allValues), Set([value, value2]))
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
}
