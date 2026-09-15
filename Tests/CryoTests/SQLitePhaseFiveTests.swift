import SQLite3
import XCTest
@testable import Cryo

private struct SQLiteWideModel: CryoModel {
    @CryoColumn var id: String = "wide"
    @CryoColumn var integer: Int = 0
    @CryoColumn var blob: Data = Data()
    @CryoColumn var date: Date = Date(timeIntervalSince1970: 0)
    @CryoAsset var asset: URL = URL(fileURLWithPath: "/tmp/asset")
}

private struct ReservedSQLiteModel: CryoModel {
    static let tableName = "select"
    @CryoColumn var id: String = "reserved"
    @CryoColumn var order: String = "ascending"
}

private struct MigrationV1: CryoModel {
    static let tableName = "migration"
    @CryoColumn var id: String = "row"
}

private struct MigrationV2: CryoModel {
    static let tableName = "migration"
    @CryoColumn var id: String = "row"
    @CryoColumn var added: Int = 0
}

final class SQLitePhaseFiveTests: CryoTestCase {
    private enum Expected: Error { case rollback }

    func testWideValuesAssetsAndReusableQueries() async throws {
        let url = environment.sqliteURL
        let sqlite = try SQLiteAdaptor(databaseUrl: url, config: environment.config)
        try await sqlite.createTable(for: SQLiteWideModel.self).execute()
        XCTAssertTrue(FileManager.default.fileExists(atPath: url.path))

        let date = Date(timeIntervalSince1970: 1_700_000_000.125)
        let value = SQLiteWideModel(integer: Int.max, blob: Data(repeating: 0xAB, count: 1_048_576),
                                    date: date, asset: URL(fileURLWithPath: "/tmp/asset value"))
        let insert = try sqlite.insert(value)
        XCTAssertTrue(try insert.execute())
        XCTAssertTrue(try insert.execute())

        let select = try sqlite.select(from: SQLiteWideModel.self)
        _ = select.queryString
        _ = try select.where("integer", equals: Int.max)
        XCTAssertTrue(select.queryString.contains("WHERE \"integer\""))
        let first = try XCTUnwrap(select.execute().first)
        XCTAssertEqual(first.integer, Int.max)
        XCTAssertEqual(first.blob, value.blob)
        XCTAssertEqual(first.date.timeIntervalSince1970, date.timeIntervalSince1970, accuracy: 0.001)
        XCTAssertEqual(first.asset, value.asset)
        XCTAssertEqual(try select.execute().count, 1)

        let empty = SQLiteWideModel(id: "empty", blob: Data())
        try sqlite.insert(empty).execute()
        XCTAssertEqual(try sqlite.select(id: empty.id, from: SQLiteWideModel.self).execute().first?.blob, Data())
    }

    func testTransactionsAttachmentAndChangeHook() async throws {
        let sqlite = try SQLiteAdaptor(databaseUrl: environment.sqliteURL)
        try await sqlite.createTable(for: SQLiteWideModel.self).execute()

        try await sqlite.transaction {
            try sqlite.insert(SQLiteWideModel(id: "committed")).execute()
        }
        XCTAssertEqual(try sqlite.select(id: "committed", from: SQLiteWideModel.self).execute().count, 1)

        do {
            try await sqlite.transaction {
                try sqlite.insert(SQLiteWideModel(id: "rolled-back")).execute()
                throw Expected.rollback
            }
            XCTFail("Expected rollback")
        } catch Expected.rollback { }
        XCTAssertEqual(try sqlite.select(id: "rolled-back", from: SQLiteWideModel.self).execute().count, 0)

        var attachedAlias: String?
        try await sqlite.withAttachedDatabase(databaseUrl: environment.root.appendingPathComponent("attached.db")) {
            attachedAlias = $0
        }
        XCTAssertEqual(attachedAlias, "cryo_attached")

        let changes = expectation(description: "update hook remains valid across row changes")
        changes.expectedFulfillmentCount = 1_003
        sqlite.registerChangeListener(for: SQLiteWideModel.self) { changes.fulfill() }
        try sqlite.insert(SQLiteWideModel(id: "hook")).execute()
        try sqlite.update(id: "hook", from: SQLiteWideModel.self).set("integer", to: 2).execute()
        try sqlite.delete(id: "hook", from: SQLiteWideModel.self).execute()
        for index in 0..<1_000 {
            try sqlite.insert(SQLiteWideModel(id: "hook-\(index)")).execute()
        }
        await fulfillment(of: [changes], timeout: 2)
    }

    func testReservedIdentifiersAndAdditiveMigration() async throws {
        let sqlite = try SQLiteAdaptor(databaseUrl: environment.sqliteURL)
        try await sqlite.createTable(for: ReservedSQLiteModel.self).execute()
        try sqlite.insert(ReservedSQLiteModel()).execute()
        XCTAssertEqual(try sqlite.select(from: ReservedSQLiteModel.self).execute().first?.order, "ascending")

        try await sqlite.createTable(for: MigrationV1.self).execute()
        try sqlite.insert(MigrationV1()).execute()
        try await sqlite.createTable(for: MigrationV2.self).execute()
        try sqlite.insert(MigrationV2(id: "new", added: 42)).execute()
        XCTAssertEqual(try sqlite.select(id: "new", from: MigrationV2.self).execute().first?.added, 42)
    }

    func testReplacePreservesCreationTimestampAndDeleteKeepsLateWhere() async throws {
        let clock = environment.clock
        let databaseURL = environment.sqliteURL
        let sqlite = try SQLiteAdaptor(databaseUrl: databaseURL, config: environment.config)
        try await sqlite.createTable(for: SQLiteWideModel.self).execute()
        try sqlite.insert(SQLiteWideModel(id: "keep", integer: 1)).execute()
        let created = try metadataDate("_cryo_created", id: "keep", databaseURL: databaseURL)
        clock.advance(by: 10)
        try sqlite.insert(SQLiteWideModel(id: "keep", integer: 2), replace: true).execute()
        XCTAssertEqual(try metadataDate("_cryo_created", id: "keep", databaseURL: databaseURL), created)

        try sqlite.insert(SQLiteWideModel(id: "other", integer: 3)).execute()
        let delete = try sqlite.delete(from: SQLiteWideModel.self)
        _ = delete.queryString
        _ = try delete.where("id", equals: "keep")
        XCTAssertEqual(try delete.execute(), 1)
        XCTAssertEqual(try sqlite.select(from: SQLiteWideModel.self).execute().map(\.id), ["other"])
    }

    private func metadataDate(_ column: String, id: String, databaseURL: URL) throws -> String {
        var connection: OpaquePointer?
        XCTAssertEqual(sqlite3_open(databaseURL.path, &connection), SQLITE_OK)
        guard let connection else { throw Expected.rollback }
        defer { sqlite3_close(connection) }
        var statement: OpaquePointer?
        let sql = "SELECT \"\(column)\" FROM \"SQLiteWideModel\" WHERE \"id\" = ?"
        XCTAssertEqual(sqlite3_prepare_v2(connection, sql, -1, &statement, nil), SQLITE_OK)
        guard let statement else { throw Expected.rollback }
        defer { sqlite3_finalize(statement) }
        let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        _ = id.withCString { sqlite3_bind_text(statement, 1, $0, -1, transient) }
        XCTAssertEqual(sqlite3_step(statement), SQLITE_ROW)
        return String(cString: sqlite3_column_text(statement, 0))
    }
}
