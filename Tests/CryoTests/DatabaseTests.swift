
import XCTest
@testable import Cryo

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
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
}
