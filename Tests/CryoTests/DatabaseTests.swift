
import XCTest
@testable import Cryo

fileprivate struct TestModel: CryoModel {
    @CryoColumn var x: Int = 0
    @CryoColumn var y: String = ""
    @CryoColumn var z: Data = .init()
    @CryoAsset var w: URL
}

extension TestModel: Equatable {
    static func ==(lhs: TestModel, rhs: TestModel) -> Bool {
        guard lhs.x == rhs.x && lhs.y == rhs.y && lhs.z == rhs.z else {
            return false
        }
        
        guard let data1 = try? Data(contentsOf: lhs.w), let data2 = try? Data(contentsOf: rhs.w) else {
            return false
        }
        
        return data1 == data2
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
        
        let value = TestModel(x: 123, y: "Hello there", z: .init(count: 50), w: assetUrl)
        do {
            let key = AnyKey(id: "test-123", for: TestModel.self)
            try await adaptor.persist(value, for: key)
            
            let loadedValue = try await adaptor.load(with: key)
            XCTAssertEqual(value, loadedValue)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
}
