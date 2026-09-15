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
    guard timeout.isFinite else { return try await operation() }

    let state = TimeoutRaceState<Value>()
    return try await withTaskCancellationHandler(operation: {
        try await withCheckedThrowingContinuation { continuation in
            state.install(continuation: continuation)
            let operationTask = Task {
                do {
                    state.resolve(.success(try await operation()))
                } catch {
                    state.resolve(.failure(error))
                }
            }
            let timerTask = Task {
                let interval = max(timeout, 0) * 1_000_000_000
                let nanoseconds = UInt64(min(interval, Double(UInt64.max)))
                do {
                    try await Task.sleep(nanoseconds: nanoseconds)
                    state.resolve(.failure(CryoTimeoutError(timeout: timeout)))
                } catch is CancellationError {
                    return
                } catch {
                    state.resolve(.failure(error))
                }
            }
            state.install(operationTask: operationTask, timerTask: timerTask)
        }
    }, onCancel: { state.cancel() })
}

private final class TimeoutRaceState<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Value, Error>?
    private var operationTask: Task<Void, Never>?
    private var timerTask: Task<Void, Never>?
    private var cancelled = false

    func install(continuation: CheckedContinuation<Value, Error>) {
        lock.lock()
        if cancelled {
            lock.unlock()
            continuation.resume(throwing: CancellationError())
            return
        }
        self.continuation = continuation
        lock.unlock()
    }

    func install(operationTask: Task<Void, Never>, timerTask: Task<Void, Never>) {
        lock.lock()
        self.operationTask = operationTask
        self.timerTask = timerTask
        let alreadyResolved = continuation == nil
        lock.unlock()
        if alreadyResolved {
            operationTask.cancel()
            timerTask.cancel()
        }
    }
    
    @discardableResult
    func resolve(_ result: Result<Value, Error>) -> Bool {
        lock.lock()
        guard let continuation else {
            lock.unlock()
            return false
        }
        self.continuation = nil
        let operationTask = self.operationTask
        let timerTask = self.timerTask
        lock.unlock()
        operationTask?.cancel()
        timerTask?.cancel()
        continuation.resume(with: result)
        return true
    }

    func cancel() {
        lock.lock()
        cancelled = true
        guard let continuation else {
            let operationTask = self.operationTask
            let timerTask = self.timerTask
            lock.unlock()
            operationTask?.cancel()
            timerTask?.cancel()
            return
        }
        self.continuation = nil
        let operationTask = self.operationTask
        let timerTask = self.timerTask
        lock.unlock()
        operationTask?.cancel()
        timerTask?.cancel()
        continuation.resume(throwing: CancellationError())
    }
}
