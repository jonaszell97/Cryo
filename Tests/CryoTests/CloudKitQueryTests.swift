import CloudKit
import XCTest
@testable import Cryo

private struct CloudQueryModel: CryoModel, Equatable {
    @CryoColumn var id: String = "row"
    @CryoColumn var score: Double = 0
    @CryoColumn var name: String = ""
    @CryoColumn var payload: Data? = nil

    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.id == rhs.id && lhs.score == rhs.score && lhs.name == rhs.name && lhs.payload == rhs.payload
    }
}

final class CloudKitQueryTests: CryoTestCase {
    private func makeCloud() async throws -> CloudKitAdaptor {
        let cloud = environment.cloud
        try await cloud.createTable(for: CloudQueryModel.self, initializeCloudKitSchema: false).execute()
        return cloud
    }

    func testDoublePredicateLimitAndIdParity() async throws {
        let cloud = try await makeCloud()
        for index in 0..<5 {
            try await cloud.insert(CloudQueryModel(id: "\(index)", score: Double(index) + 0.25,
                                                   name: "name-\(index)")).execute()
        }
        let matches = try await cloud.select(from: CloudQueryModel.self)
            .where("score", isGreatherThan: 1.5).sort(by: "score", .descending).limit(2).execute()
        XCTAssertEqual(matches.map(\.id), ["4", "3"])
        XCTAssertEqual(environment.database.fetchLog.last?.resultsLimit, 2)
        let missing = try await cloud.select(id: "missing", from: CloudQueryModel.self).execute()
        XCTAssertEqual(missing, [])
        let filteredID = try await cloud.select(id: "2", from: CloudQueryModel.self)
            .where("score", isGreatherThan: 3.0).execute()
        XCTAssertEqual(filteredID, [])
    }

    func testInsertUpdateAndDeleteInspectPerRecordResults() async throws {
        let cloud = try await makeCloud()
        let value = CloudQueryModel(id: "failure", score: 1.25, name: "before", payload: Data([1, 2]))
        try await cloud.insert(value).execute()

        do {
            _ = try await cloud.insert(value, replace: false).execute()
            XCTFail("Expected duplicate")
        } catch CryoError.duplicateId(let id) {
            XCTAssertEqual(id, value.id)
        }

        environment.database.saveFailures[.init(recordName: value.id)] = CKError(.permissionFailure)
        do {
            _ = try await cloud.update(id: value.id, from: CloudQueryModel.self).set("name", to: "after").execute()
            XCTFail("Expected save failure")
        } catch {
            XCTAssertEqual((error as? CKError)?.code, .permissionFailure)
        }
        environment.database.saveFailures.removeAll()

        let missingUpdate = try await cloud.update(id: "missing", from: CloudQueryModel.self)
            .set("name", to: "unused").execute()
        XCTAssertEqual(missingUpdate, 0)
        let updated = try await cloud.update(id: value.id, from: CloudQueryModel.self)
            .set("payload", to: Data?.none).execute()
        XCTAssertEqual(updated, 1)
        let loaded = try await cloud.select(id: value.id, from: CloudQueryModel.self).execute()
        XCTAssertNil(loaded.first?.payload)

        environment.database.deleteFailures[.init(recordName: value.id)] = CKError(.permissionFailure)
        do {
            _ = try await cloud.delete(id: value.id, from: CloudQueryModel.self).execute()
            XCTFail("Expected delete failure")
        } catch {
            XCTAssertEqual((error as? CKError)?.code, .permissionFailure)
        }
        environment.database.deleteFailures.removeAll()
        let deleted = try await cloud.delete(id: value.id, from: CloudQueryModel.self).execute()
        XCTAssertEqual(deleted, 1)
        let missingDelete = try await cloud.delete(id: value.id, from: CloudQueryModel.self).execute()
        XCTAssertEqual(missingDelete, 0)
    }

    func testCreateTableUsesDeterministicDisposableRecord() async throws {
        let cloud = environment.cloud
        let dummyID = CKRecord.ID(recordName: "_cryo_schema_\(CloudQueryModel.tableName)")
        let stale = CKRecord(recordType: CloudQueryModel.tableName, recordID: dummyID)
        stale["id"] = dummyID.recordName as NSString
        stale["score"] = 0 as NSNumber
        stale["name"] = "stale" as NSString
        _ = try await environment.database.modifyRecords(saving: [stale], deleting: [], savePolicy: .changedKeys)

        try await cloud.createTable(for: CloudQueryModel.self, initializeCloudKitSchema: true).execute()
        let rows = try await cloud.select(from: CloudQueryModel.self).execute()
        XCTAssertEqual(rows, [])
    }

    func testRetryRecognizesPartialFailureAndForwardsLogger() async throws {
        let nested = CKError(.zoneBusy)
        let partial = CKError(.partialFailure, userInfo: [CKPartialErrorsByItemIDKey: ["row": nested]])
        var attempts = 0
        var delays: [TimeInterval] = []
        var messages: [String] = []
        let value: Int = try await CloudKitAdaptor.cloudKitOperation(
            maxAttempts: 1, defaultDelay: 0.25,
            log: { _, message in messages.append(message) },
            sleep: { delays.append($0) }
        ) {
            attempts += 1
            if attempts == 1 { throw partial }
            return 7
        }
        XCTAssertEqual(value, 7)
        XCTAssertEqual(delays, [0.25])
        XCTAssertEqual(messages.count, 1)
    }
}
