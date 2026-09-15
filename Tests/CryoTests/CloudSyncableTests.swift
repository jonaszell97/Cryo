#if canImport(UIKit)

import Toolbox
import XCTest
@testable import Cryo

@MainActor final class CloudSyncableTests: CryoTestCase {
    func testMissingLocalCreatesButCorruptLocalReturnsAnErrorWithoutOverwrite() async throws {
        let stores = CloudSyncStores<SyncValue>(local: environment.documents, remote: nil)
        let created = try await SyncValue.loadInstanceReportingErrors(
            withIdentifier: "device", in: stores, loadRemoteInstances: false
        )
        XCTAssertEqual(created.identifier, "device")

        let key = SyncKey(deviceIdentifier: "corrupt")
        let url = try environment.documents.documentUrl(for: key)
        let corrupt = Data("not-json".utf8)
        try await environment.documents.persist(corrupt, url: url)
        let failed = await SyncValue.loadInstance(withIdentifier: "corrupt", in: stores,
                                                  loadRemoteInstances: false)
        XCTAssertNil(failed)
        XCTAssertEqual(try Data(contentsOf: url), corrupt)
    }

    func testSubsecondComparisonAndIdentifierExclusion() async throws {
        let older = SyncValue(identifier: "older", modified: Date(timeIntervalSince1970: 10.1))
        let newer = SyncValue(identifier: "newer", modified: Date(timeIntervalSince1970: 10.2))
        XCTAssertGreaterThan(SyncValue.compare(lhs: newer, rhs: older), 0)

        try await environment.cloud.createTable(for: SyncModel.self, initializeCloudKitSchema: false).execute()
        try await environment.cloud.insert(try SyncModel(value: older)).execute()
        try await environment.cloud.insert(try SyncModel(value: newer)).execute()
        let stores = CloudSyncStores<SyncValue>(local: environment.documents, remote: environment.cloud)
        let remote = try await SyncValue.loadRemoteInstances(excludingIdentifier: "older", in: stores)
        XCTAssertEqual(remote.map(\.identifier), ["newer"])
    }

    func testMergeDoesNotDeleteRemoteDeviceRecords() async throws {
        try await environment.cloud.createTable(for: SyncModel.self, initializeCloudKitSchema: false).execute()
        let local = SyncValue(identifier: "local", modified: Date(timeIntervalSince1970: 10))
        let remoteA = SyncValue(identifier: "a", modified: Date(timeIntervalSince1970: 11))
        let remoteB = SyncValue(identifier: "b", modified: Date(timeIntervalSince1970: 12))
        for value in [remoteA, remoteB] {
            try await environment.cloud.insert(try SyncModel(value: value)).execute()
        }
        let stores = CloudSyncStores<SyncValue>(local: environment.documents, remote: environment.cloud)
        let merged = await local.mergeWithRemoteInstances(in: stores)
        XCTAssertTrue(merged)
        let records = try await environment.cloud.select(from: SyncModel.self).execute()
        XCTAssertEqual(Set(records.map(\.deviceIdentifier)), ["a", "b"])
        XCTAssertEqual(local.payload, "b")
    }
}

private struct SyncKey: CloudSyncableKey {
    typealias Value = SyncValue
    let id: String
    init(deviceIdentifier: String) { id = "sync-\(deviceIdentifier)" }
    static func ownsInstanceWithKey(_ key: String) -> Bool { key.hasPrefix("sync-") }
    static func deviceIdentifierFromKey(_ key: String) -> String { String(key.dropFirst(5)) }
}

private struct SyncModel: CloudSyncableModel {
    @CryoColumn var id: String
    @CryoColumn var deviceIdentifier: String
    @CryoColumn var data: Data

    init(value: SyncValue) throws {
        id = Self.identifier(for: value.identifier)
        deviceIdentifier = value.identifier
        data = try JSONEncoder().encode(value)
    }

    init() { id = ""; deviceIdentifier = ""; data = Data() }
    func createInstance() throws -> SyncValue { try JSONDecoder().decode(SyncValue.self, from: data) }
    static func identifier(for deviceIdentifier: String) -> String { "sync-model-\(deviceIdentifier)" }
}

private final class SyncValue: CloudSyncable {
    typealias ModelType = SyncModel
    typealias LocalKey = SyncKey
    typealias LocalStore = DocumentAdaptor
    typealias RemoteStore = CloudKitAdaptor

    static let logger = Logger(subsystem: "CryoTests", category: "CloudSyncable")
    nonisolated let identifier: String
    nonisolated(unsafe) var lastModificationDate: Date
    nonisolated(unsafe) var payload: String

    init(identifier: String) {
        self.identifier = identifier
        self.lastModificationDate = .distantPast
        self.payload = identifier
    }

    init(identifier: String, modified: Date) {
        self.identifier = identifier
        self.lastModificationDate = modified
        self.payload = identifier
    }

    private enum CodingKeys: String, CodingKey { case identifier, lastModificationDate, payload }

    nonisolated required init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        identifier = try values.decode(String.self, forKey: .identifier)
        lastModificationDate = try values.decode(Date.self, forKey: .lastModificationDate)
        payload = try values.decode(String.self, forKey: .payload)
    }

    nonisolated func encode(to encoder: Encoder) throws {
        var values = encoder.container(keyedBy: CodingKeys.self)
        try values.encode(identifier, forKey: .identifier)
        try values.encode(lastModificationDate, forKey: .lastModificationDate)
        try values.encode(payload, forKey: .payload)
    }

    func consolidate(source: SyncValue) {
        lastModificationDate = source.lastModificationDate
        payload = source.payload
    }
}

#endif
