
import XCTest
@testable import Cryo

final class PropertyWrapperTests: XCTestCase {
    private static var userDefaults: UserDefaults? = nil
    private static var userDefaultsAdaptor: UserDefaultsAdaptor? = nil
    
    override func setUp() {
        super.setUp()
        
        Self.userDefaults = UserDefaults(suiteName: "CryoTests_PropertyWrapperTests")
        Self.userDefaults?.removePersistentDomain(forName: "CryoTests_PropertyWrapperTests")
        
        Self.userDefaultsAdaptor = .init(defaults: Self.userDefaults!)
    }
    
    func testUserDefaults() async {
        struct TestStruct {
            @CryoPersisted("testValue1", adaptor: PropertyWrapperTests.userDefaultsAdaptor!) var testValue1: Int = 0
            @CryoPersisted("testValue2", adaptor: PropertyWrapperTests.userDefaultsAdaptor!) var testValue2: String = "hello"
            @CryoPersisted("testValue3", adaptor: PropertyWrapperTests.userDefaultsAdaptor!) var testValue3: Date = .distantPast
            @CryoPersisted("testValue4", saveOnWrite: false, adaptor: PropertyWrapperTests.userDefaultsAdaptor!) var testValue4: Int = 12
            
            var testValue4Wrapper: CryoPersisted<Int> { _testValue4 }
        }
        
        do {
            var myStruct = TestStruct()
            XCTAssertEqual(0, myStruct.testValue1)
            XCTAssertEqual("hello", myStruct.testValue2)
            XCTAssertEqual(Date.distantPast, myStruct.testValue3)
            XCTAssertEqual(12, myStruct.testValue4)
            
            myStruct.testValue1 = 17
            XCTAssertEqual(17, myStruct.testValue1)
            
            myStruct.testValue4 = 37
            XCTAssertEqual(37, myStruct.testValue4)
        }
        
        try? await Task.sleep(nanoseconds: 1)
        
        do {
            var myStruct = TestStruct()
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
        
        try? await Task.sleep(nanoseconds: 1)
        
        do {
            let myStruct = TestStruct()
            XCTAssertEqual(37, myStruct.testValue4)
        }
    }
}
