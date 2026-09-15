import Foundation

internal protocol UbiquitousMetadataItem {
    func value(forAttribute key: String) -> Any?
}
extension NSMetadataItem: UbiquitousMetadataItem {}

internal enum UbiquitousMetadataEvent {
    case gathered([any UbiquitousMetadataItem])
    case updated([any UbiquitousMetadataItem])
}

internal protocol UbiquitousMetadataQuerying: AnyObject {
    var events: AsyncStream<UbiquitousMetadataEvent> { get }
    @MainActor func start(predicate: NSPredicate, sortDescriptors: [NSSortDescriptor], scopes: [String]) -> Bool
    @MainActor func stop()
}

/// Owns all NotificationCenter plumbing for a single metadata query.
/// Mutable query state is confined to MainActor methods and main-queue observers.
/// Only the immutable event stream is consumed from other executors.
internal final class SystemUbiquitousMetadataQuery: UbiquitousMetadataQuerying, @unchecked Sendable {
    let events: AsyncStream<UbiquitousMetadataEvent>
    private let continuation: AsyncStream<UbiquitousMetadataEvent>.Continuation
    private let query = NSMetadataQuery()
    private var observers: [NSObjectProtocol] = []

    init() {
        var continuation: AsyncStream<UbiquitousMetadataEvent>.Continuation!
        events = AsyncStream { continuation = $0 }
        self.continuation = continuation
    }

    @MainActor func start(predicate: NSPredicate, sortDescriptors: [NSSortDescriptor], scopes: [String]) -> Bool {
        query.predicate = predicate
        query.sortDescriptors = sortDescriptors
        query.searchScopes = scopes
        query.operationQueue = .main
        for name in [Notification.Name.NSMetadataQueryDidFinishGathering, .NSMetadataQueryDidUpdate] {
            observers.append(NotificationCenter.default.addObserver(forName: name, object: query, queue: .main) { [weak self] _ in
                guard let self else { return }
                self.query.disableUpdates()
                let items = self.query.results.compactMap { $0 as? any UbiquitousMetadataItem }
                self.query.enableUpdates()
                self.continuation.yield(name == .NSMetadataQueryDidFinishGathering ? .gathered(items) : .updated(items))
            })
        }
        return query.start()
    }

    @MainActor func stop() {
        query.stop()
        observers.forEach(NotificationCenter.default.removeObserver)
        observers.removeAll()
        continuation.finish()
    }

    deinit {
        observers.forEach(NotificationCenter.default.removeObserver)
        continuation.finish()
    }
}

/// Makes cancellation safe even when it arrives before the search task is installed.
internal final class MetadataSearchLifetime: @unchecked Sendable {
    private let lock = NSLock()
    private var task: Task<Void, Never>?
    private var cancelled = false
    func install(_ task: Task<Void, Never>) {
        lock.lock()
        defer { lock.unlock() }
        self.task = task
        if cancelled { task.cancel() }
    }
    func cancel() {
        lock.lock()
        defer { lock.unlock() }
        cancelled = true
        task?.cancel()
    }
}
