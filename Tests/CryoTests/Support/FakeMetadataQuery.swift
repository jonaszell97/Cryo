import Foundation
@testable import Cryo

struct FakeMetadataItem: UbiquitousMetadataItem {
    var attributes: [String: Any]
    func value(forAttribute key: String) -> Any? { attributes[key] }
}

final class FakeMetadataQuery: UbiquitousMetadataQuerying {
    let events: AsyncStream<UbiquitousMetadataEvent>
    private let continuation: AsyncStream<UbiquitousMetadataEvent>.Continuation
    var startsSuccessfully = true
    var initialEvents: [UbiquitousMetadataEvent] = []
    private(set) var started = false
    private(set) var stopped = false
    private(set) var predicate: NSPredicate?
    init() {
        var continuation: AsyncStream<UbiquitousMetadataEvent>.Continuation!
        events = AsyncStream { continuation = $0 }
        self.continuation = continuation
    }
    func send(_ event: UbiquitousMetadataEvent) { continuation.yield(event) }
    @MainActor func start(predicate: NSPredicate, sortDescriptors: [NSSortDescriptor], scopes: [String]) -> Bool {
        started = true
        self.predicate = predicate
        initialEvents.forEach(send)
        return startsSuccessfully
    }
    @MainActor func stop() { stopped = true; continuation.finish() }
}
