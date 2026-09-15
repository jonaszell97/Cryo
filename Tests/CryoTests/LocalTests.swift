import XCTest
@testable import Cryo

final class CryoLocalTests: CryoTestCase {
    struct AnyKey<Value: Codable>: CryoKey {
        let id: String
        
        init(id: String) {
            self.id = id
        }

        init(id: String, for: Value.Type) {
            self.id = id
        }
    }
    
    struct MyCodableStruct: Codable, Equatable {
        var x: Int = 0
        var y: String = ""
        var z: Date = .distantPast
    }
    
    func adaptorTest(for adaptor: CryoSynchronousAdaptor) async {
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
            XCTAssertEqual(8493123, try adaptor.loadSynchronously(with: intKey))
            
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
            XCTAssertEqual([1,2,3], arrayValue)
            XCTAssertEqual([1,2,3], try adaptor.loadSynchronously(with: arrayKey))
            
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
        let adaptor = environment.keyValueStore
        await self.adaptorTest(for: adaptor)
    }
    
    func testLocalDocumentAdaptor() async {
        let adaptor = environment.documents
        await self.adaptorTest(for: adaptor)
    }
}
