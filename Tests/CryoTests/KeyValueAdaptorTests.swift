import XCTest
@testable import Cryo

final class KeyValueAdaptorTests: CryoTestCase {
    func testOptionalValuesRoundTripThroughBothAdaptors() async throws {
        let optionalInt = CryoNamedKey(id: "optional-int", for: Int?.self)
        let optionalDate = CryoNamedKey(id: "optional-date", for: Date?.self)
        let date = Date(timeIntervalSinceReferenceDate: 123.25)

        try await environment.keyValueStore.persist(42, for: optionalInt)
        try await environment.keyValueStore.persist(date, for: optionalDate)
        XCTAssertEqual(try environment.keyValueStore.loadSynchronously(with: optionalInt), 42)
        XCTAssertEqual(try environment.keyValueStore.loadSynchronously(with: optionalDate), date)

        let fake = FakeUbiquitousKeyValueStore()
        let ubiquitous = UbiquitousKeyValueStoreAdaptor(store: fake)
        try await ubiquitous.persist(42, for: optionalInt)
        try await ubiquitous.persist(date, for: optionalDate)
        XCTAssertEqual(try ubiquitous.loadSynchronously(with: optionalInt), 42)
        XCTAssertEqual(try ubiquitous.loadSynchronously(with: optionalDate), date)

        try await ubiquitous.persist(nil, for: optionalInt)
        let removed: Int?? = try ubiquitous.loadSynchronously(with: optionalInt)
        XCTAssertNil(removed as Any?)
    }

    func testPrefixedRemoveAllDoesNotTouchForeignKeys() async throws {
        let defaults = environment.defaults
        defaults.set("foreign", forKey: "foreign")
        let adaptor = UserDefaultsAdaptor(defaults: defaults, keyPrefix: "cryo.")
        let key = CryoNamedKey(id: "value", for: String.self)
        try await adaptor.persist("owned", for: key)
        try await adaptor.removeAll()
        XCTAssertEqual(defaults.string(forKey: "foreign"), "foreign")
        XCTAssertNil(defaults.object(forKey: "cryo.value"))

        let fake = FakeUbiquitousKeyValueStore()
        fake.set("foreign", forKey: "foreign")
        let ubiquitous = UbiquitousKeyValueStoreAdaptor(store: fake, keyPrefix: "cryo.")
        try await ubiquitous.persist("owned", for: key)
        try await ubiquitous.removeAll()
        XCTAssertEqual(fake.string(forKey: "foreign"), "foreign")
        XCTAssertNil(fake.object(forKey: "cryo.value"))
    }

    func testObserverUsesInjectedStoreAndCanBeRemoved() {
        let fake = FakeUbiquitousKeyValueStore()
        let adaptor = UbiquitousKeyValueStoreAdaptor(store: fake)
        var calls = 0
        let id = adaptor.observeChanges { _ in calls += 1 }
        NotificationCenter.default.post(
            name: NSUbiquitousKeyValueStore.didChangeExternallyNotification,
            object: fake,
            userInfo: [NSUbiquitousKeyValueStoreChangedKeysKey: ["value"]]
        )
        XCTAssertEqual(calls, 1)
        adaptor.removeObserver(withId: id)
        NotificationCenter.default.post(name: NSUbiquitousKeyValueStore.didChangeExternallyNotification,
                                        object: fake)
        XCTAssertEqual(calls, 1)
    }
}
