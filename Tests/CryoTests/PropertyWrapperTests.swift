
import XCTest
@testable import Cryo

final class PropertyWrapperTests: CryoTestCase {
    func testUserDefaults() async throws {
        struct TestStruct {
            @CryoPersisted var testValue1: Int
            @CryoPersisted var testValue2: String
            @CryoPersisted var testValue3: Date
            @CryoPersisted var testValue4: Int
            
            init(adaptor: UserDefaultsAdaptor) {
                _testValue1 = .init(defaultValue: 0, "testValue1", adaptor: adaptor)
                _testValue2 = .init(defaultValue: "hello", "testValue2", adaptor: adaptor)
                _testValue3 = .init(defaultValue: .distantPast, "testValue3", adaptor: adaptor)
                _testValue4 = .init(defaultValue: 12, "testValue4", saveOnWrite: false, adaptor: adaptor)
            }
            func persistFirstValue() async throws { try await _testValue1.persist() }
            var testValue4Wrapper: CryoPersisted<Int> { _testValue4 }
        }
        
        do {
            var myStruct = TestStruct(adaptor: environment.keyValueStore)
            XCTAssertEqual(0, myStruct.testValue1)
            XCTAssertEqual("hello", myStruct.testValue2)
            XCTAssertEqual(Date.distantPast, myStruct.testValue3)
            XCTAssertEqual(12, myStruct.testValue4)
            
            myStruct.testValue1 = 17
            try await myStruct.persistFirstValue()
            XCTAssertEqual(17, myStruct.testValue1)
            
            myStruct.testValue4 = 37
            XCTAssertEqual(37, myStruct.testValue4)
        }
        
        
        do {
            var myStruct = TestStruct(adaptor: environment.keyValueStore)
            XCTAssertEqual(17, myStruct.testValue1)
            XCTAssertEqual("hello", myStruct.testValue2)
            XCTAssertEqual(Date.distantPast, myStruct.testValue3)
            XCTAssertEqual(12, myStruct.testValue4)
            
            myStruct.testValue4 = 37
            try await myStruct.testValue4Wrapper.persist()
        }
        catch {
            XCTAssert(false)
        }
        
        
        do {
            let myStruct = TestStruct(adaptor: environment.keyValueStore)
            XCTAssertEqual(37, myStruct.testValue4)
        }
    }
}
