import XCTest
@testable import Cryo

private enum ContractState: String, CaseIterable, CryoColumnStringValue { case ready, done }
private struct OptionalModel: CryoModel {
    @CryoColumn var id: String = "optional"
    @CryoColumn var integer: Int?
    @CryoColumn var double: Double?
    @CryoColumn var string: String?
    @CryoColumn var date: Date?
    @CryoColumn var data: Data?
    @CryoColumn var url: URL?
    @CryoColumn var uuid: UUID?
    @CryoColumn var state: ContractState?
}

final class DatabaseAdaptorContractTests: CryoTestCase {
    func testOptionalColumnsOnSQLiteAndCloudKit() async throws {
        let sqlite = try SQLiteAdaptor(databaseUrl: environment.sqliteURL, config: environment.config)
        let cloud = environment.cloud
        try await sqlite.createTable(for: OptionalModel.self).execute()
        try await cloud.createTable(for: OptionalModel.self, initializeCloudKitSchema: false).execute()
        let models = [OptionalModel(), OptionalModel(data: Data()), OptionalModel(integer: 42, double: 1.5, string: "text",
            date: Date(timeIntervalSince1970: 1_700_000_000), data: Data([1, 2, 3]),
            url: URL(fileURLWithPath: "/tmp/value"), uuid: UUID(), state: .done)]
        for model in models {
            try sqlite.insert(model, replace: true).execute()
            try await cloud.insert(model, replace: true).execute()
            let local = try sqlite.select(from: OptionalModel.self).execute()
            let remote = try await cloud.select(from: OptionalModel.self).execute()
            for rows in [local, remote] {
                let row = try XCTUnwrap(rows.first)
                XCTAssertEqual(row.integer, model.integer)
                XCTAssertEqual(row.double, model.double)
                XCTAssertEqual(row.string, model.string)
                XCTAssertEqual(row.date, model.date)
                XCTAssertEqual(row.data, model.data)
                XCTAssertEqual(row.url, model.url)
                XCTAssertEqual(row.uuid, model.uuid)
                XCTAssertEqual(row.state, model.state)
            }
        }
        try sqlite.update(from: OptionalModel.self).set("integer", to: Int?.none).execute()
        try await cloud.update(from: OptionalModel.self).set("integer", to: Int?.none).execute()
        XCTAssertNil(try sqlite.select(from: OptionalModel.self).execute().first?.integer)
        let remote = try await cloud.select(from: OptionalModel.self).execute()
        XCTAssertNil(remote.first?.integer)
    }

    func testSQLiteReplayPreservesReplaceAndNull() async throws {
        let sqlite = try SQLiteAdaptor(databaseUrl: environment.sqliteURL, config: environment.config)
        try await sqlite.createTable(for: OptionalModel.self).execute()
        let insert = try sqlite.insert(OptionalModel(), replace: false)
        let operation = try await insert.operation
        try await sqlite.execute(operation: operation)
        do {
            try await sqlite.execute(operation: operation)
            XCTFail("Expected duplicate")
        } catch CryoError.duplicateId { }
        let replacement = try await sqlite.insert(OptionalModel(integer: 7), replace: true).operation
        try await sqlite.execute(operation: replacement)
        XCTAssertEqual(try sqlite.select(from: OptionalModel.self).execute().first?.integer, 7)
    }
}
