import Foundation
import XCTest
@testable import Cryo

final class ManualClock: @unchecked Sendable {
    private let lock = NSLock()
    private var date: Date
    init(_ date: Date = Date(timeIntervalSince1970: 1_700_000_000)) { self.date = date }
    func now() -> Date { lock.lock(); defer { lock.unlock() }; return date }
    func advance(by interval: TimeInterval = 1) { lock.lock(); defer { lock.unlock() }; date.addTimeInterval(interval) }
}

final class CryoTestEnvironment {
    let root: URL
    let suiteName = "CryoTests.\(UUID().uuidString)"
    let defaults: UserDefaults
    let clock = ManualClock()
    let database = InMemoryCloudKitDatabase()
    let documents: DocumentAdaptor
    let keyValueStore: UserDefaultsAdaptor
    var sqliteURL: URL { root.appendingPathComponent("\(UUID().uuidString).db") }
    var config: CryoConfig { var config = CryoConfig(); config.now = { [clock] in clock.now() }; return config }
    lazy var cloud = CloudKitAdaptor(config: config, database: database, userRecordID: "test-user")

    init() throws {
        root = FileManager.default.temporaryDirectory.appendingPathComponent("CryoTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defaults = UserDefaults(suiteName: suiteName)!
        keyValueStore = UserDefaultsAdaptor(defaults: defaults)
        let documentRoot = root.appendingPathComponent("documents", isDirectory: true)
        try FileManager.default.createDirectory(at: documentRoot, withIntermediateDirectories: true)
        documents = DocumentAdaptor(config: .init(), url: documentRoot, usesUbiquitousStorage: false)
    }

    func tearDown() throws {
        defaults.removePersistentDomain(forName: suiteName)
        try FileManager.default.removeItem(at: root)
    }
}

class CryoTestCase: XCTestCase {
    var environment: CryoTestEnvironment!
    override func setUpWithError() throws { environment = try CryoTestEnvironment() }
    override func tearDownWithError() throws { try environment.tearDown(); environment = nil }
}
