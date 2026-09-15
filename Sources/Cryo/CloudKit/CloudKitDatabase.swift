import CloudKit

/// Opaque continuation owned by the database that produced it.
internal struct CloudKitQueryCursor {
    let token: Any
}

internal typealias CloudKitRecordPage = (
    matchResults: [(CKRecord.ID, Result<CKRecord, Error>)],
    queryCursor: CloudKitQueryCursor?
)
internal typealias CloudKitModifyResults = (
    saveResults: [CKRecord.ID: Result<CKRecord, Error>],
    deleteResults: [CKRecord.ID: Result<Void, Error>]
)

/// The CloudKit transport boundary. Query construction and decoding stay in Cryo.
internal protocol CloudKitDatabase {
    func record(for id: CKRecord.ID) async throws -> CKRecord
    func records(matching query: CKQuery, resultsLimit: Int) async throws -> CloudKitRecordPage
    func records(continuingMatchFrom cursor: CloudKitQueryCursor, resultsLimit: Int) async throws -> CloudKitRecordPage
    func modifyRecords(saving: [CKRecord], deleting: [CKRecord.ID], savePolicy: CKModifyRecordsOperation.RecordSavePolicy) async throws -> CloudKitModifyResults
    @discardableResult func save(_ subscription: CKSubscription) async throws -> CKSubscription
}

internal extension CloudKitDatabase {
    func records(matching query: CKQuery) async throws -> CloudKitRecordPage {
        try await records(matching: query, resultsLimit: CKQueryOperation.maximumResults)
    }
    func records(continuingMatchFrom cursor: CloudKitQueryCursor) async throws -> CloudKitRecordPage {
        try await records(continuingMatchFrom: cursor, resultsLimit: CKQueryOperation.maximumResults)
    }
    func modifyRecords(saving: [CKRecord], deleting: [CKRecord.ID]) async throws -> CloudKitModifyResults {
        try await modifyRecords(saving: saving, deleting: deleting, savePolicy: .ifServerRecordUnchanged)
    }
}

extension CKDatabase: CloudKitDatabase {
    internal func records(matching query: CKQuery, resultsLimit: Int) async throws -> CloudKitRecordPage {
        let page = try await records(matching: query, inZoneWith: nil, desiredKeys: nil, resultsLimit: resultsLimit)
        return (page.matchResults, page.queryCursor.map { CloudKitQueryCursor(token: $0) })
    }
    internal func records(continuingMatchFrom cursor: CloudKitQueryCursor, resultsLimit: Int) async throws -> CloudKitRecordPage {
        guard let cursor = cursor.token as? CKQueryOperation.Cursor else {
            throw CKError(.invalidArguments)
        }
        let page = try await records(continuingMatchFrom: cursor, desiredKeys: nil, resultsLimit: resultsLimit)
        return (page.matchResults, page.queryCursor.map { CloudKitQueryCursor(token: $0) })
    }
    internal func modifyRecords(saving: [CKRecord], deleting: [CKRecord.ID], savePolicy: CKModifyRecordsOperation.RecordSavePolicy) async throws -> CloudKitModifyResults {
        try await modifyRecords(saving: saving, deleting: deleting, savePolicy: savePolicy, atomically: true)
    }
}
