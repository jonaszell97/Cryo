import XCTest
@testable import Cryo

private enum SchemaState: String, CaseIterable, CryoColumnStringValue { case ready, done }
private struct SchemaModel: CryoModel {
    @CryoColumn var id: String
    @CryoColumn var state: SchemaState
}
private struct InvalidSchemaModel: CryoModel { var id: String { "computed" } }
private struct InvalidIdModel: CryoModel {
    @CryoColumn var other: String
    var id: String { other }
}
private enum NoDefaultState: String, CryoColumnStringValue { case ready }
private struct NoDefaultModel: CryoModel {
    @CryoColumn var id: String
    @CryoColumn var state: NoDefaultState
}
private struct FailingModel: CryoModel {
    var id: String { "id" }
    init(from decoder: Decoder) throws { throw CryoError.invalidModel(message: "Cannot decode") }
    func encode(to encoder: Encoder) throws {}
}

final class SchemaTests: XCTestCase {
    func testSchemaErrorsAndStringEnumDefault() throws {
        let manager = CryoSchemaManager()
        XCTAssertThrowsError(try manager.schema(for: SchemaModel.self))
        XCTAssertThrowsError(try manager.schema(tableName: SchemaModel.tableName))
        try manager.createSchema(for: SchemaModel.self)
        XCTAssertEqual(try manager.schema(for: SchemaModel.self).columns.map(\.columnName), ["id", "state"])
        XCTAssertThrowsError(try InvalidSchemaModel.schema)
        XCTAssertThrowsError(try InvalidIdModel.schema)
        XCTAssertThrowsError(try NoDefaultModel.schema)
        XCTAssertThrowsError(try FailingModel.schema) { error in
            guard case CryoError.invalidModel = error else { return XCTFail("\(error)") }
        }
        let schema = try SchemaModel.schema
        XCTAssertThrowsError(try schema.create(["id": .init(value: "id"), "state": .init(value: "unknown")]))
    }

    func testConcurrentCreationAndLookupOffMain() async throws {
        let manager = CryoSchemaManager()
        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<100 {
                group.addTask {
                    try manager.createSchema(for: SchemaModel.self)
                    XCTAssertEqual(try manager.schema(for: SchemaModel.self).columns.count, 2)
                    XCTAssertEqual(try manager.schema(tableName: SchemaModel.tableName).columns.count, 2)
                }
            }
            try await group.waitForAll()
        }
    }

    private enum Key: String, CodingKey { case value, missing }

    func testDecoderThrowsForMissingMismatchedAndNestedValues() throws {
        let decoder = CryoModelDecoder(data: ["value": .init(value: "text")])
        let container = try decoder.container(keyedBy: Key.self)
        XCTAssertThrowsError(try container.decode(String.self, forKey: .missing)) { error in
            guard case DecodingError.keyNotFound = error else { return XCTFail("\(error)") }
        }
        XCTAssertThrowsError(try container.decode(Int.self, forKey: .value)) { error in
            guard case DecodingError.typeMismatch = error else { return XCTFail("\(error)") }
        }
        XCTAssertThrowsError(try decoder.unkeyedContainer())
        XCTAssertThrowsError(try decoder.singleValueContainer())
        XCTAssertThrowsError(try container.nestedContainer(keyedBy: Key.self, forKey: .value))
        XCTAssertThrowsError(try container.nestedUnkeyedContainer(forKey: .value))
        XCTAssertThrowsError(try container.superDecoder())
        XCTAssertThrowsError(try container.superDecoder(forKey: .value))
        let valueDecoder = CryoModelValueDecoder(value: .init(value: "text"))
        XCTAssertThrowsError(try valueDecoder.container(keyedBy: Key.self))
        XCTAssertThrowsError(try valueDecoder.unkeyedContainer())
    }

    func testUInt64AndIntegerRangeDecoding() throws {
        for value: Int64 in [0, 42, .max] {
            let container = try CryoModelDecoder(data: ["value": .init(value: value)]).container(keyedBy: Key.self)
            XCTAssertEqual(try container.decode(UInt64.self, forKey: .value), UInt64(value))
            let scalar = try CryoModelValueDecoder(value: .init(value: value)).singleValueContainer()
            XCTAssertEqual(try scalar.decode(UInt64.self), UInt64(value))
        }
        let negative = try CryoModelValueDecoder(value: .init(value: Int64(-1))).singleValueContainer()
        XCTAssertThrowsError(try negative.decode(UInt64.self))
        let large = try CryoModelValueDecoder(value: .init(value: Int64.max)).singleValueContainer()
        XCTAssertThrowsError(try large.decode(Int8.self))
    }
}
