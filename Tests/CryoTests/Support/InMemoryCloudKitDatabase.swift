import CloudKit
@testable import Cryo

/// A transport fake: production Cryo queries still build predicates and encode/decode records.
final class InMemoryCloudKitDatabase: CloudKitDatabase {
    var isOnline = true
    var pageSize = 2
    var failures: [CKRecord.ID: Error] = [:]
    var recordFailures: [CKRecord.ID: Error] = [:]
    var saveFailures: [CKRecord.ID: Error] = [:]
    var deleteFailures: [CKRecord.ID: Error] = [:]
    var nextOperationErrors: [Error] = []
    private(set) var fetchLog: [(query: CKQuery, resultsLimit: Int)] = []
    private(set) var continuationLimits: [Int] = []
    private(set) var subscriptions: [CKSubscription] = []
    private var rows: [CKRecord.ID: CKRecord] = [:]
    private var versions: [CKRecord.ID: Int] = [:]
    private let fetchedVersions = NSMapTable<CKRecord, NSNumber>(keyOptions: .weakMemory, valueOptions: .strongMemory)
    private struct Cursor {
        let records: [CKRecord]
        let offset: Int
    }

    static func rateLimit(retryAfter: TimeInterval) -> CKError {
        CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: retryAfter])
    }

    private func checkOperation() throws {
        if !nextOperationErrors.isEmpty { throw nextOperationErrors.removeFirst() }
        guard isOnline else { throw CKError(.networkUnavailable) }
    }

    private func snapshot(_ record: CKRecord) -> CKRecord {
        let copy = record.copy() as! CKRecord
        fetchedVersions.setObject(NSNumber(value: versions[record.recordID, default: 0]), forKey: copy)
        return copy
    }

    func record(for id: CKRecord.ID) async throws -> CKRecord {
        try checkOperation()
        if let error = recordFailures[id] ?? failures[id] { throw error }
        guard let record = rows[id] else { throw CKError(.unknownItem) }
        return snapshot(record)
    }

    func records(matching query: CKQuery, resultsLimit: Int) async throws -> CloudKitRecordPage {
        try checkOperation()
        fetchLog.append((query, resultsLimit))
        func dictionary(_ record: CKRecord) -> NSMutableDictionary {
            let row = NSMutableDictionary()
            for key in record.allKeys() { row[key] = record[key] }
            row["recordID"] = record.recordID
            return row
        }
        // CKQuery copies predicates through secure coding and disables evaluation.
        let predicate = query.predicate
        predicate.allowEvaluation()
        var matching = rows.values.filter {
            $0.recordType == query.recordType && predicate.evaluate(with: dictionary($0))
        }.sorted { $0.recordID.recordName < $1.recordID.recordName }
        if let descriptors = query.sortDescriptors, !descriptors.isEmpty {
            matching.sort { lhs, rhs in
                for descriptor in descriptors {
                    let result = descriptor.compare(dictionary(lhs), to: dictionary(rhs))
                    if result != .orderedSame { return result == .orderedAscending }
                }
                return lhs.recordID.recordName < rhs.recordID.recordName
            }
        }
        return page(Cursor(records: matching.map(snapshot), offset: 0), limit: resultsLimit)
    }

    func records(continuingMatchFrom cursor: CloudKitQueryCursor, resultsLimit: Int) async throws -> CloudKitRecordPage {
        try checkOperation()
        continuationLimits.append(resultsLimit)
        guard let cursor = cursor.token as? Cursor else { throw CKError(.invalidArguments) }
        return page(cursor, limit: resultsLimit)
    }

    private func page(_ cursor: Cursor, limit: Int) -> CloudKitRecordPage {
        let count = min(max(1, pageSize), limit == CKQueryOperation.maximumResults ? Int.max : max(1, limit))
        let end = min(cursor.offset + count, cursor.records.count)
        let results: [(CKRecord.ID, Result<CKRecord, Error>)] = cursor.records[cursor.offset..<end].map {
            ($0.recordID, failures[$0.recordID].map { .failure($0) } ?? .success($0))
        }
        let next = end < cursor.records.count ? CloudKitQueryCursor(token: Cursor(records: cursor.records, offset: end)) : nil
        return (results, next)
    }

    func modifyRecords(saving: [CKRecord], deleting: [CKRecord.ID], savePolicy: CKModifyRecordsOperation.RecordSavePolicy) async throws -> CloudKitModifyResults {
        try checkOperation()
        var saved: [CKRecord.ID: Result<CKRecord, Error>] = [:]
        var deleted: [CKRecord.ID: Result<Void, Error>] = [:]
        for record in saving {
            let id = record.recordID
            if let error = saveFailures[id] ?? failures[id] { saved[id] = .failure(error); continue }
            if savePolicy == .ifServerRecordUnchanged, rows[id] != nil,
               fetchedVersions.object(forKey: record)?.intValue != versions[id] {
                saved[id] = .failure(CKError(.serverRecordChanged)); continue
            }
            let stored: CKRecord
            if savePolicy == .changedKeys, let existing = rows[id] {
                stored = existing.copy() as! CKRecord
                for key in record.changedKeys() { stored[key] = record[key] }
            } else { stored = record.copy() as! CKRecord }
            rows[id] = stored
            versions[id, default: 0] += 1
            saved[id] = .success(snapshot(stored))
        }
        for id in deleting {
            if let error = deleteFailures[id] ?? failures[id] { deleted[id] = .failure(error); continue }
            guard rows.removeValue(forKey: id) != nil else { deleted[id] = .failure(CKError(.unknownItem)); continue }
            versions.removeValue(forKey: id)
            deleted[id] = .success(())
        }
        return (saved, deleted)
    }

    func save(_ subscription: CKSubscription) async throws -> CKSubscription {
        try checkOperation()
        subscriptions.append(subscription)
        return subscription
    }
}
