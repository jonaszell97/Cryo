
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
        }
        
        do {
            var myStruct = TestStruct()
            XCTAssertEqual(0, myStruct.testValue1)
            XCTAssertEqual("hello", myStruct.testValue2)
            XCTAssertEqual(Date.distantPast, myStruct.testValue3)
            
            myStruct.testValue1 = 17
            XCTAssertEqual(17, myStruct.testValue1)
        }
        
        try? await Task.sleep(nanoseconds: 1)
        
        do {
            let myStruct = TestStruct()
            XCTAssertEqual(17, myStruct.testValue1)
            XCTAssertEqual("hello", myStruct.testValue2)
            XCTAssertEqual(Date.distantPast, myStruct.testValue3)
        }
    }
}
