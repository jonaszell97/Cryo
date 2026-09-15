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

    func testCallerCancellationWinsImmediately() async {
        let operationCancelled = expectation(description: "operation cancelled")
        let task = Task {
            try await withCryoTimeout(10) {
                do {
                    try await Task.sleep(nanoseconds: 10_000_000_000)
                    return 1
                } catch is CancellationError {
                    operationCancelled.fulfill()
                    throw CancellationError()
                }
            }
        }
        task.cancel()
        do {
            _ = try await task.value
            XCTFail("Expected cancellation")
        } catch is CancellationError { }
        catch { XCTFail("Unexpected error: \(error)") }
        await fulfillment(of: [operationCancelled], timeout: 1)
    }

    func testInfiniteTimeoutRunsOperation() async throws {
        let result = try await withCryoTimeout(.infinity) { 7 }
        XCTAssertEqual(result, 7)
    }

    func testZeroTimeoutDoesNotWaitForNonCooperativeOperation() async {
        let start = Date()
        do {
            _ = try await withCryoTimeout(0) {
                await withCheckedContinuation { continuation in
                    DispatchQueue.global().asyncAfter(deadline: .now() + 1) {
                        continuation.resume(returning: 1)
                    }
                }
            }
            XCTFail("Expected timeout")
        } catch is CryoTimeoutError {
            XCTAssertLessThan(Date().timeIntervalSince(start), 0.2)
        } catch { XCTFail("Unexpected error: \(error)") }
    }
}
