import XCTest
@testable import Cryo

final class CryoLocalTests: XCTestCase {
    struct AnyKey<Value: Codable>: CryoKey {
        let id: String
        
        init(id: String, for: Value.Type) {
            self.id = id
        }
    }
    
    struct MyCodableStruct: Codable, CryoDatabaseValue, Equatable {
        var x: Int = 0
        var y: String = ""
        var z: Date = .distantPast
    }
    
    private var userDefaults: UserDefaults? = nil
    
    override func setUp() {
        super.setUp()
        
        userDefaults = UserDefaults(suiteName: "CryoTestsSuite")
        userDefaults?.removePersistentDomain(forName: "CryoTestsSuite")
    }
    
    func adaptorTest(for adaptor: CryoAdaptor) async {
        do {
            // Remove All
            try await adaptor.removeAll()
            
            // Integers
            let intKey = AnyKey(id: "testInt", for: Int.self)
            var intValue = try await adaptor.load(with: intKey)
            XCTAssertNil(intValue)
            
            try await adaptor.persist(102, for: intKey)
            intValue = try await adaptor.load(with: intKey)
            XCTAssertEqual(102, intValue)
            
            try await adaptor.persist(8493123, for: intKey)
            intValue = try await adaptor.load(with: intKey)
            XCTAssertEqual(8493123, intValue)
            
            // Strings
            let stringKey = AnyKey(id: "testString", for: String.self)
            var stringValue = try await adaptor.load(with: stringKey)
            XCTAssertNil(stringValue)
            
            try await adaptor.persist("Hello 123", for: stringKey)
            stringValue = try await adaptor.load(with: stringKey)
            XCTAssertEqual("Hello 123", stringValue)
            
            // Codable
            let codableStruct = MyCodableStruct(x: 1, y: "hi", z: .now)
            let codableKey = AnyKey(id: "testCodable", for: MyCodableStruct.self)
            try await adaptor.persist(codableStruct, for: codableKey)
            
            let loadedCodableStruct = try await adaptor.load(with: codableKey)
            XCTAssertEqual(codableStruct, loadedCodableStruct)
            
            // Remove single
            try await adaptor.remove(with: intKey)
            
            intValue = try await adaptor.load(with: intKey)
            XCTAssertNil(intValue)
            
            stringValue = try await adaptor.load(with: stringKey)
            XCTAssertEqual("Hello 123", stringValue)
            
            // Arrays
            let arrayKey = AnyKey(id: "testArray", for: [Int].self)
            try await adaptor.persist([1,2,3], for: arrayKey)
            let arrayValue = try await adaptor.load(with: arrayKey)
            XCTAssertEqual(arrayValue, [1,2,3])
            
            // Remove All
            try await adaptor.removeAll()
            
            stringValue = try await adaptor.load(with: stringKey)
            XCTAssertNil(stringValue)
        }
        catch {
            XCTAssert(false, error.localizedDescription)
        }
    }
    
    func testUserDefaultsAdaptor() async {
        guard let userDefaults else {
            XCTAssert(false, "failed to set up UserDefaults")
            return
        }
        
        let adaptor = UserDefaultsAdaptor(defaults: userDefaults)
        await self.adaptorTest(for: adaptor)
    }
    
    func testLocalDocumentAdaptor() async {
        let adaptor = DocumentAdaptor.local()
        await self.adaptorTest(for: adaptor)
    }
}
