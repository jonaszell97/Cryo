
import XCTest
@testable import Cryo

fileprivate struct TestModel: CryoModel {
    @CryoColumn var x: Int = 0
    @CryoColumn var y: String = ""
    @CryoColumn var z: Data = .init()
}

extension TestModel: Equatable {
    static func ==(lhs: TestModel, rhs: TestModel) -> Bool {
        lhs.x == rhs.x && lhs.y == rhs.y && lhs.z == rhs.z
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
        let value = TestModel(x: 123, y: "Hello there", z: .init(count: 50))
        
        do {
            let key = AnyKey(id: "test-123", for: TestModel.self)
            try await adaptor.persist(value, for: key)
            
            let loadedValue = try await adaptor.load(with: key)
            XCTAssertEqual(value, loadedValue)
            
            print(adaptor.database)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
}
