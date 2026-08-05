import Foundation

/// An error raised when an asynchronous Cryo operation exceeds its deadline.
public struct CryoTimeoutError: Error, Equatable, Sendable {
    public let timeout: TimeInterval
    
    public init(timeout: TimeInterval) {
        self.timeout = timeout
    }
}

/// Race an asynchronous operation against a deadline.
///
/// The operation runs in an unstructured task. Timing out only cancels that task; APIs that do
/// not cooperate with cancellation may still finish later without delaying the caller.
public func withCryoTimeout<Value: Sendable>(
    _ timeout: TimeInterval,
    operation: @escaping @Sendable () async throws -> Value
) async throws -> Value {
    try await withCheckedThrowingContinuation { continuation in
        let state = TimeoutRaceState(continuation: continuation)
        let operationTask = Task {
            do {
                state.resolve(.success(try await operation()))
            }
            catch {
                state.resolve(.failure(error))
            }
        }
        
        Task {
            let nanoseconds = UInt64(max(timeout, 0) * 1_000_000_000)
            try? await Task.sleep(nanoseconds: nanoseconds)
            if state.resolve(.failure(CryoTimeoutError(timeout: timeout))) {
                operationTask.cancel()
            }
        }
    }
}

private final class TimeoutRaceState<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Value, Error>?
    
    init(continuation: CheckedContinuation<Value, Error>) {
        self.continuation = continuation
    }
    
    @discardableResult
    func resolve(_ result: Result<Value, Error>) -> Bool {
        lock.lock()
        guard let continuation else {
            lock.unlock()
            return false
        }
        self.continuation = nil
        lock.unlock()
        continuation.resume(with: result)
        return true
    }
}
