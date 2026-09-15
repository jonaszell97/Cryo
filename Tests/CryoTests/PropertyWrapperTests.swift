
import XCTest
@testable import Cryo

final class PropertyWrapperTests: CryoTestCase {
    func testWritesAreSerializedAndFlushWaits() async throws {
        let adaptor = SlowIntAdaptor()
        var wrapper = CryoPersisted(wrappedValue: 0, "ordered", adaptor: adaptor)
        wrapper.wrappedValue = 1
        wrapper.wrappedValue = 2
        wrapper.wrappedValue = 3
        try await wrapper.flush()
        XCTAssertEqual(adaptor.values, [1, 2, 3])
    }

    func testLoadErrorSuppressesAutoSaveUntilExplicitPersist() async throws {
        let adaptor = SlowIntAdaptor(loadError: TestPersistenceError.failed)
        var errors = 0
        var wrapper = CryoPersisted(wrappedValue: 10, "corrupt", adaptor: adaptor) { _ in errors += 1 }
        XCTAssertEqual(errors, 1)
        wrapper.wrappedValue = 11
        try await wrapper.flush()
        XCTAssertTrue(adaptor.values.isEmpty)
        try await wrapper.persist()
        XCTAssertEqual(adaptor.values, [11])
    }

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

private enum TestPersistenceError: Error { case failed }

private final class SlowIntAdaptor: CryoSynchronousAdaptor {
    private let lock = NSLock()
    private var stored: Int?
    private let loadError: Error?
    private(set) var values: [Int] = []

    init(loadError: Error? = nil) { self.loadError = loadError }

    private func withLock<T>(_ body: () -> T) -> T {
        lock.lock(); defer { lock.unlock() }
        return body()
    }

    func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws {
        let delay = UInt64(max(0, 4 - ((value as? Int) ?? 0))) * 2_000_000
        try await Task.sleep(nanoseconds: delay)
        try persistSynchronously(value, for: key)
    }

    func persistSynchronously<Key: CryoKey>(_ value: Key.Value?, for key: Key) throws {
        guard let value = value as? Int else { return }
        withLock { values.append(value); stored = value }
    }

    func loadSynchronously<Key: CryoKey>(with key: Key) throws -> Key.Value? {
        if let loadError { throw loadError }
        return withLock { stored as? Key.Value }
    }

    func removeAll() async throws { try removeAllSynchronously() }
    func removeAllSynchronously() throws { withLock { values = []; stored = nil } }
}
