import XCTest
@testable import Cryo

final class OperationCodableTests: CryoTestCase {
    func testUnqualifiedOperationsAndNullRoundTrip() throws {
        let date = Date(timeIntervalSinceReferenceDate: 17)
        let operations: [DatabaseOperation] = [
            .update(date: date, tableName: "T", rowId: nil, setClauses: [.init(columnName: "name", value: .null)], whereClauses: []),
            .delete(date: date, tableName: "T", rowId: nil, whereClauses: [])]
        let encoder = JSONEncoder()
        encoder.outputFormatting = .sortedKeys
        for operation in operations {
            let data = try encoder.encode(operation)
            let decoded = try JSONDecoder().decode(DatabaseOperation.self, from: data)
            XCTAssertEqual(try encoder.encode(decoded), data)
        }
    }

    func testInsertReplaceAndLegacyPayload() throws {
        for replace in [true, false] {
            let operation = DatabaseOperation.insert(date: .distantPast, tableName: "T", rowId: "id", data: [.init(columnName: "optional", value: .null)], replace: replace)
            let decoded = try JSONDecoder().decode(DatabaseOperation.self, from: JSONEncoder().encode(operation))
            guard case .insert(_, _, _, let values, let actual) = decoded else { return XCTFail() }
            XCTAssertEqual(actual, replace)
            XCTAssertEqual(values.first?.value, .null)
        }
        let legacy = Data(#"{"insert":{"_0":0,"_1":"T","_2":"id","_3":[]}}"#.utf8)
        guard case .insert(_, _, _, _, let replace) = try JSONDecoder().decode(DatabaseOperation.self, from: legacy) else { return XCTFail() }
        XCTAssertTrue(replace)
        for kind in ["update", "delete"] {
            let clauses = kind == "update" ? #", "_4":[]"# : ""
            let data = Data("{\"\(kind)\":{\"_0\":0,\"_1\":\"T\",\"_3\":[]\(clauses)}}".utf8)
            XCTAssertNoThrow(try JSONDecoder().decode(DatabaseOperation.self, from: data))
        }
    }

    func testReplacePropagatesThroughQueryWrappers() async throws {
        let cloud = environment.cloud
        try await cloud.createTable(for: SeamTestModel.self, initializeCloudKitSchema: false).execute()
        let query = try cloud.insert(SeamTestModel(), replace: false)
        let resilient = ResilientInsertQuery(query: query, onExecutionFailed: { false })
        let synchronized = SynchronizedInsertQuery(query: resilient, onExecutionCompleted: {})
        guard case .insert(_, _, _, _, let replace) = try await synchronized.operation else { return XCTFail() }
        XCTAssertFalse(replace)
    }

    func testSyncOperationDataRoundTrip() throws {
        let operation = DatabaseOperation.delete(date: .distantPast, tableName: "T", rowId: nil, whereClauses: [])
        let sync = try SyncOperation(storeIdentifier: "store", deviceIdentifier: "device", date: .distantPast, operation: operation)
        let decoded = try JSONDecoder().decode(SyncOperation.self, from: JSONEncoder().encode(sync))
        XCTAssertEqual(decoded.operationData, sync.operationData)
        guard case .delete(_, let table, let id, _) = try decoded.operation else { return XCTFail() }
        XCTAssertEqual(table, "T")
        XCTAssertNil(id)
    }

    func testDescriptionNamesColumnsAndConjoinsConditions() {
        let op = DatabaseOperation.update(date: .distantPast, tableName: "T", rowId: nil,
            setClauses: [.init(columnName: "name", value: .null)],
            whereClauses: [.init(columnName: "a", operation: .equals, value: .integer(value: 1)),
                           .init(columnName: "b", operation: .equals, value: .integer(value: 2))])
        XCTAssertTrue(op.description.contains("name ="))
        XCTAssertTrue(op.description.contains(" AND b ="))
    }
}
