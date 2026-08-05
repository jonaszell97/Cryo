import XCTest
@testable import Cryo

final class TimeoutTests: XCTestCase {
    func testOperationCompletesBeforeTimeout() async throws {
        let result = try await withCryoTimeout(1) { 42 }
        XCTAssertEqual(result, 42)
    }
    
    func testTimeoutReturnsWithinBudget() async {
        let start = Date.now
        do {
            _ = try await withCryoTimeout(0.02) {
                try await Task.sleep(nanoseconds: 5_000_000_000)
                return 42
            }
            XCTFail("expected timeout")
        }
        catch let error as CryoTimeoutError {
            XCTAssertEqual(error.timeout, 0.02)
            XCTAssertLessThan(Date.now.timeIntervalSince(start), 0.5)
        }
        catch {
            XCTFail("unexpected error: \(error)")
        }
    }
    
    func testOperationErrorIsForwarded() async {
        struct ExpectedError: Error { }
        do {
            _ = try await withCryoTimeout(1) { () async throws -> Int in
                throw ExpectedError()
            }
            XCTFail("expected operation error")
        }
        catch is ExpectedError {
        }
        catch {
            XCTFail("unexpected error: \(error)")
        }
    }
}
