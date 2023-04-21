
import CloudKit
import Foundation

// MARK: SynchronizedStoreBackend

internal protocol SynchronizedStoreBackend {
    /// Create a table.
    func createTable<Model: CryoModel>(for model: Model.Type) async throws -> any CryoCreateTableQuery<Model>
    
    /// Persist a sync operation.
    func persist(operation: SyncOperation) async throws
    
    /// Load sync operations after a given date.
    func loadOperations(after: Date,
                        storeIdentifier: String,
                        deviceIdentifier: String) async throws -> [SyncOperation]
    
    /// Load a sync operation with the given ID.
    func loadOperation(withId id: String) async throws -> SyncOperation?
    
    /// Setup a subscription to be notified of record changes.
    func setupRecordChangeSubscription(for tableName: String,
                                       storeIdentifier: String,
                                       deviceIdentifier: String,
                                       callback: @escaping (String?) async throws -> Void) async throws
}

extension SQLiteAdaptor: SynchronizedStoreBackend {
    /// Persist a sync operation.
    internal func persist(operation: SyncOperation) async throws {
        try await self.insert(operation, replace: false).execute()
    }
    
    /// Load sync operations after a given date.
    internal func loadOperations(after date: Date,
                                 storeIdentifier: String,
                                 deviceIdentifier: String) async throws -> [SyncOperation] {
        try await self
            .select(from: SyncOperation.self)
            .where("date", isGreatherThan: date.timeIntervalSinceReferenceDate)
            .and("storeIdentifier", equals: storeIdentifier)
            .and("deviceIdentifier", doesNotEqual: deviceIdentifier)
            .execute()
    }
    
    /// Load a sync operation with the given ID.
    internal func loadOperation(withId id: String) async throws -> SyncOperation? {
        try await self.select(id: id, from: SyncOperation.self).execute().first
    }
    
    /// Setup a subscription to be notified of record changes.
    internal func setupRecordChangeSubscription(for tableName: String,
                                                storeIdentifier: String,
                                                deviceIdentifier: String,
                                                callback: @escaping (String?) async throws -> Void) async throws {
        self.registerChangeListener(tableName: tableName) {
            try await callback(nil)
        }
    }
}

extension CloudKitAdaptor: SynchronizedStoreBackend {
    /// Persist a sync operation.
    internal func persist(operation: SyncOperation) async throws {
        try await self.insert(operation, replace: false).execute()
    }
    
    /// Load sync operations after a given date.
    internal func loadOperations(after date: Date,
                                 storeIdentifier: String,
                                 deviceIdentifier: String) async throws -> [SyncOperation] {
        try await self
            .select(from: SyncOperation.self)
            .where("date", isGreatherThan: date.timeIntervalSinceReferenceDate)
            .and("storeIdentifier", equals: storeIdentifier)
            .and("deviceIdentifier", doesNotEqual: deviceIdentifier)
            .execute()
    }
    
    /// Load a sync operation with the given ID.
    internal func loadOperation(withId id: String) async throws -> SyncOperation? {
        try await self.select(id: id, from: SyncOperation.self).execute().first
    }
    
    /// Setup a subscription to be notified of record changes.
    internal func setupRecordChangeSubscription(for tableName: String,
                                                storeIdentifier: String,
                                                deviceIdentifier: String,
                                                callback: @escaping (String?) async throws -> Void) async throws {
        // Subscribe to changes from other devices for this store
        let predicate = NSPredicate(format: "storeIdentifier == %@ AND deviceIdentifier != %@",
                                    storeIdentifier, deviceIdentifier)
        
        let subscription = CKQuerySubscription(recordType: tableName,
                                               predicate: predicate,
                                               subscriptionID: "\(storeIdentifier)-operation-changes",
                                               options: .firesOnRecordCreation)
        
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true
        subscription.notificationInfo = notificationInfo
        
        // Save the subscription
        let operation = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription],
                                                       subscriptionIDsToDelete: nil)
        
        return try await withCheckedThrowingContinuation { continuation in
            operation.modifySubscriptionsResultBlock = { result in
                if case .failure(let error) = result {
                    continuation.resume(throwing: error)
                    return
                }
            }
            
            operation.qualityOfService = .background
            CKContainer.default().privateCloudDatabase.add(operation)
        }
    }
    
    /// Callback invoked when a CloudKit notification is received.
    public func receivedCloudKitNotification(_ notification: CKQueryNotification, store: SynchronizedStore) async throws {
        guard case .recordCreated = notification.queryNotificationReason else {
            config.log?(.debug, "received non-created notification: \(notification.queryNotificationReason.rawValue)")
            return
        }
        
        try await store.externalChangeNotificationReceived(recordId: notification.recordID?.recordName)
    }
}
