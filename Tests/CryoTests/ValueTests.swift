import XCTest
@testable import Cryo

private enum StringValue: String, CaseIterable, CryoColumnStringValue { case ready, done }
private enum IntValue: Int, CryoColumnIntValue {
    case ready = 42
    static let defaultValue = IntValue.ready
}
private enum DoubleValue: Double, CryoColumnDoubleValue {
    case ready = 1.5
    static let defaultValue = DoubleValue.ready
}

final class ValueTests: XCTestCase {
    func testOptionalValues() throws {
        let nils: [_AnyCryoColumnValue] = [Int?.none, Double?.none, String?.none,
            Date?.none, Data?.none, URL?.none, UUID?.none, StringValue?.none]
        for value in nils { XCTAssertEqual(try CryoQueryValue(value: value), .null) }
        let date = Date(timeIntervalSince1970: 123.25)
        let values: [(_AnyCryoColumnValue, CryoQueryValue)] = [
            (Int?.some(42), .integer(value: 42)), (Double?.some(1.5), .double(value: 1.5)),
            (String?.some("a"), .string(value: "a")), (Date?.some(date), .date(value: date)),
            (Data?.some(Data([1, 2])), .data(value: Data([1, 2]))),
            (URL?.some(URL(fileURLWithPath: "/tmp/a")), .string(value: "file:///tmp/a")),
            (UUID?.some(UUID.defaultValue), .string(value: UUID.defaultValue.uuidString)),
            (StringValue?.some(.done), .string(value: "done"))]
        for (value, expected) in values { XCTAssertEqual(try CryoQueryValue(value: value), expected) }
    }

    func testNullAndLegacyCoding() throws {
        let encoder = JSONEncoder()
        XCTAssertEqual(String(decoding: try encoder.encode(CryoQueryValue.null), as: UTF8.self), "{\"null\":true}")
        for value: CryoQueryValue in [.null, .integer(value: 4), .string(value: "old"), .double(value: 1.5), .date(value: .distantPast), .data(value: Data([1])), .asset(value: URL(fileURLWithPath: "/tmp/a"))] {
            XCTAssertEqual(try JSONDecoder().decode(CryoQueryValue.self, from: encoder.encode(value)), value)
        }
        XCTAssertEqual(try JSONDecoder().decode(CryoQueryValue.self, from: Data(#"{"integer":17}"#.utf8)), .integer(value: 17))
        XCTAssertThrowsError(try JSONDecoder().decode(CryoQueryValue.self, from: Data(#"{"null":false}"#.utf8)))
    }

    func testInvalidStoredValuesThrow() throws {
        XCTAssertEqual(try StringValue(stringValue: "done"), .done)
        XCTAssertEqual(try IntValue(integerValue: 42), .ready)
        XCTAssertEqual(try DoubleValue(doubleValue: 1.5), .ready)
        XCTAssertThrowsError(try StringValue(stringValue: "unknown"))
        XCTAssertThrowsError(try IntValue(integerValue: 0))
        XCTAssertThrowsError(try DoubleValue(doubleValue: 0))
        XCTAssertThrowsError(try URL(stringValue: "http://["))
        XCTAssertThrowsError(try UUID(stringValue: "invalid"))
        XCTAssertThrowsError(try Int8(integerValue: 128))
        XCTAssertThrowsError(try UInt8(integerValue: -1))
    }
}
