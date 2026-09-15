
import Foundation

/// An implementation of ``CryoAdaptor`` that persists values as documents.
///
/// This adaptor stores values by encoding them using a `JSONEncoder` and writing the resulting `Data` to a file.
/// Two shared instances of this provider are available. ``DocumentAdaptor/sharedLocal`` stores values in the
/// a folder named `.cryo` within the App's document directory.
///
/// ``DocumentAdaptor/cloud(fileManager:)`` can be used to create an adaptor instance that stores values in
/// the user's iCloud documents directory. This call will fail if the user is not logged in to iCloud or if iCloud is not available
/// for some other reason.
///
/// ```swift
/// let adaptor = DocumentAdaptor.sharedLocal
/// try await adaptor.persist(3, CryoNamedKey(id: "intValue", for: Int.self))
/// try await adaptor.persist("Hi there", CryoNamedKey(id: "stringValue", for: String.self))
/// try await adaptor.persist(Date.now, CryoNamedKey(id: "dateValue", for: Date.self))
/// ```
public struct DocumentAdaptor {
    internal var makeMetadataQuery: () -> any UbiquitousMetadataQuerying = { SystemUbiquitousMetadataQuery() }
    internal var coordinate: (URL, Bool, @escaping (URL) -> Void) throws -> Void = { url, writing, accessor in
        let coordinator = NSFileCoordinator()
        var coordinationError: NSError?
        if writing {
            coordinator.coordinate(writingItemAt: url, options: [], error: &coordinationError, byAccessor: accessor)
        } else {
            coordinator.coordinate(readingItemAt: url, options: [], error: &coordinationError, byAccessor: accessor)
        }
        if let coordinationError { throw coordinationError }
    }

    /// The cryo config.
    public let config: CryoConfig
    
    /// The URL documents should be saved to.
    public let url: URL
    
    /// The file manager instance to use.
    public let fileManager: FileManager
    
    /// Whether or not this adaptor uses iCloud ubiquitous storage.
    public let usesUbiquitousStorage: Bool

    /// Whether this adaptor owns the contents of `url` and may clear them.
    public let ownsDirectory: Bool
    
    /// Create a document adaptor.
    ///
    /// - Parameters:
    ///   - url: The URL to the directory where data should be stored.
    ///   - fileManager: The file manager instance to use for file operations.
    public init(config: CryoConfig, url: URL, usesUbiquitousStorage: Bool,
                ownsDirectory: Bool = false, fileManager: FileManager = .default) {
        self.config = config
        self.url = url
        self.fileManager = fileManager
        self.usesUbiquitousStorage = usesUbiquitousStorage
        self.ownsDirectory = ownsDirectory
    }
    
    /// The shared local document adaptor.
    public static let sharedLocal: DocumentAdaptor = .local(config: CryoConfig())
    
    /// Create a local document adaptor.
    ///
    /// - Parameter fileManager: The file manager instance to use for file operations.
    /// - Returns: A document adaptor using the local documents URL.
    public static func local(config: CryoConfig, subdirectory: String? = ".cryo", fileManager: FileManager = .default) -> DocumentAdaptor {
        let documentDirectory = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)[0]
        
        var url = URL(fileURLWithPath: documentDirectory)
        if let subdirectory {
            url = url.appendingPathComponent(subdirectory)
        }
        
        do {
            try fileManager.createDirectory(at: url, withIntermediateDirectories: true)
        } catch {
            config.log?(.error, "[DocumentAdaptor.local] failed to create directory: \(error)")
        }
        return DocumentAdaptor(config: config, url: url, usesUbiquitousStorage: false,
                               ownsDirectory: subdirectory != nil, fileManager: fileManager)
    }
    
    /// Create an iCloud based document adaptor.
    ///
    /// - Parameter fileManager: The file manager instance to use for file operations.
    /// - Returns: A document adaptor using the iCloud documents URL, or `nil` if iCloud is not available.
    public static func cloud(config: CryoConfig, subdirectory: String? = ".cryo", fileManager: FileManager = .default) -> DocumentAdaptor? {
        guard fileManager.ubiquityIdentityToken != nil else {
            return nil
        }
        
        guard var containerUrl = fileManager.url(forUbiquityContainerIdentifier: nil)?.appendingPathComponent("Documents") else {
            return nil
        }
        
        if let subdirectory {
            containerUrl = containerUrl.appendingPathComponent(subdirectory)
        }
        
        do {
            try fileManager.createDirectory(at: containerUrl, withIntermediateDirectories: true)
        } catch {
            config.log?(.error, "[DocumentAdaptor.cloud] failed to create directory: \(error)")
        }
        return DocumentAdaptor(config: config, url: containerUrl, usesUbiquitousStorage: true,
                               ownsDirectory: subdirectory != nil, fileManager: fileManager)
    }

    /// Create an iCloud based document adaptor without performing ubiquity I/O on
    /// the caller's actor.
    ///
    /// - Parameter fileManager: The file manager instance to use for file operations.
    /// - Returns: A document adaptor using the iCloud documents URL, or `nil` if iCloud is not available.
    public static func cloud(
        config: CryoConfig,
        subdirectory: String? = ".cryo",
        fileManager: FileManager = .default
    ) async -> DocumentAdaptor? {
        await Task.detached(priority: .userInitiated) {
            Self.cloud(config: config, subdirectory: subdirectory, fileManager: fileManager)
        }.value
    }
}

extension DocumentAdaptor: CryoAdaptor, CryoSynchronousAdaptor {
    func documentUrl<Key: CryoKey>(for key: Key) throws -> URL {
        guard !key.id.contains("/"), !key.id.contains("\\"), !key.id.contains("..") else {
            throw CocoaError(.fileWriteInvalidFileName)
        }
        return self.url.appendingPathComponent(key.id)
    }
    
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws {
        var data: Data? = nil
        if let value {
            data = try JSONEncoder().encode(value)
        }
        
        try await self.persist(data, url: self.documentUrl(for: key))
    }
    
    public func persistSynchronously<Key: CryoKey>(_ value: Key.Value?, for key: Key) throws {
        var data: Data? = nil
        if let value {
            data = try JSONEncoder().encode(value)
        }
        
        try self.persistSynchronously(data, url: self.documentUrl(for: key))
    }
    
    public func persist(_ data: Data?, url: URL) async throws {
        config.log?(.info, "[DocumentAdaptor.persist] to \(url)")
        
        return try await withCheckedThrowingContinuation { continuation in
            config.log?(.info, "[DocumentAdaptor.persist] in continuation")
            Task.detached(priority: .userInitiated) {
                config.log?(.info, "[DocumentAdaptor.persist] in detached task")
                do {
                    try self.persistSynchronously(data, url: url)
                    config.log?(.info, "[DocumentAdaptor.persist] resume")
                    continuation.resume(returning: ())
                }
                catch {
                    config.log?(.info, "[DocumentAdaptor.persist] throw error")
                    continuation.resume(throwing: error)
                }
            }
        }
    }
    
    public func persistSynchronously(_ data: Data?, url: URL) throws {
        config.log?(.info, "[DocumentAdaptor.persistSynchronously] to \(url)")
        
        var writeError: Error? = nil
        let access: (URL) -> Void = { coordinatedURL in
            config.log?(.info, "[DocumentAdaptor.persistSynchronously] in coordination callback")
            do {
                if let data {
                    config.log?(.info, "[DocumentAdaptor.persistSynchronously] write \(data.count) bytes")
                    try data.write(to: coordinatedURL, options: .atomic)
                }
                else {
                    do {
                        try self.fileManager.removeItem(at: coordinatedURL)
                    } catch let error as CocoaError where error.code == .fileNoSuchFile {
                        // Removing a missing value is a successful no-op.
                    }
                }
                
                config.log?(.info, "[DocumentAdaptor.persistSynchronously] completed coordination callback")
            }
            catch {
                writeError = error
                config.log?(.info, "[DocumentAdaptor.persistSynchronously] error in coordination callback: \(error)")
            }
        }

        if usesUbiquitousStorage {
            try coordinate(url, true, access)
        } else {
            access(url)
        }
        
        // Check outside the closure to see if an error occurred
        if let error = writeError {
            config.log?(.info, "[DocumentAdaptor.persistSynchronously] throwing write error: \(error)")
            throw error
        }
        
    }
    
    public func remove<Key: CryoKey>(key: Key) async throws {
        try await self.persist(nil, url: self.documentUrl(for: key))
    }
    
    public func removeSynchronously<Key: CryoKey>(with key: Key) throws {
        try self.persistSynchronously(nil, url: self.documentUrl(for: key))
    }
    
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value? {
        return try await withCheckedThrowingContinuation { continuation in
            Task.detached(priority: .userInitiated) {
                do {
                    let value = try self.loadSynchronously(with: key)
                    config.log?(.info, "[DocumentAdaptor.load] resume")
                    continuation.resume(returning: value)
                }
                catch {
                    config.log?(.info, "[DocumentAdaptor.load] throw error")
                    continuation.resume(throwing: error)
                }
            }
        }
    }
    
    public func loadSynchronously<Key: CryoKey>(with key: Key) throws -> Key.Value? {
        let documentUrl = try self.documentUrl(for: key)
        guard let data = try loadDataSynchronously(for: documentUrl) else { return nil }
        return try JSONDecoder().decode(Key.Value.self, from: data)
    }

    /// Read raw document data without decoding it.
    public func loadData(for url: URL) async throws -> Data? {
        try await Task.detached(priority: .userInitiated) {
            try loadDataSynchronously(for: url)
        }.value
    }

    func loadDataSynchronously(for documentUrl: URL) throws -> Data? {
        var readError: Error? = nil
        var data: Data? = nil

        let access: (URL) -> Void = { coordinatedURL in
            do {
                data = try Data(contentsOf: coordinatedURL)
            } catch {
                if (error as NSError).code != NSFileReadNoSuchFileError {
                    readError = error
                }
            }
        }

        if usesUbiquitousStorage {
            try coordinate(documentUrl, false, access)
        } else {
            access(documentUrl)
        }
        
        // Check outside the closure to see if an error occurred
        if let error = readError {
            throw error
        }
        
        return data
    }
    
    public func removeAll() async throws {
        guard ownsDirectory else {
            throw CryoError.featureNotAvailable(message: "removeAll requires an adaptor that owns a scoped directory")
        }
        let urls = try fileManager.contentsOfDirectory(at: self.url, includingPropertiesForKeys: [.isDirectoryKey])
        for url in urls {
            guard try url.resourceValues(forKeys: [.isDirectoryKey]).isDirectory != true else { continue }
            try await self.persist(nil, url: url)
        }
    }
    
    public func removeAllSynchronously() throws {
        guard ownsDirectory else {
            throw CryoError.featureNotAvailable(message: "removeAll requires an adaptor that owns a scoped directory")
        }
        let urls = try fileManager.contentsOfDirectory(at: self.url, includingPropertiesForKeys: [.isDirectoryKey])
        for url in urls {
            guard try url.resourceValues(forKeys: [.isDirectoryKey]).isDirectory != true else { continue }
            try self.persistSynchronously(nil, url: url)
        }
    }
    
    public func removeAll(matching condition: (URL) -> Bool) async throws {
        guard ownsDirectory else {
            throw CryoError.featureNotAvailable(message: "removeAll requires an adaptor that owns a scoped directory")
        }
        let urls = try fileManager.contentsOfDirectory(at: self.url, includingPropertiesForKeys: [.isDirectoryKey])
        for url in urls {
            guard condition(url) else { continue }
            guard try url.resourceValues(forKeys: [.isDirectoryKey]).isDirectory != true else { continue }
            try await self.persist(nil, url: url)
        }
    }
    
    /// List all instances available in this adaptor.
    ///
    /// - Note: This method is not available in all adaptors.
    public func listInstanceKeys() async throws -> [String]? {
        try fileManager.contentsOfDirectory(
            at: self.url, includingPropertiesForKeys: nil
        ).map { $0.lastPathComponent }
    }
    
    /// List all instances available in this adaptor.
    ///
    /// - Note: This method is not available in all adaptors.
    public func listInstanceKeysSynchronously() throws -> [String]? {
        try fileManager.contentsOfDirectory(
            at: self.url, includingPropertiesForKeys: nil
        ).map { $0.lastPathComponent }
    }
}

extension DocumentAdaptor {
    public enum UbiquitousItemUpdate {
        /// A new value that has been fetched.
        case item(_ value: UbiquitousDocumentMetadata)
        
        /// An update on the overall download progress.
        case progressUpdate(downloaded: Int, total: Int)
    }
    
    public func loadUbiquitousDocuments(at url: URL,
                                        filenameMatching filenamePattern: String? = nil,
                                        onUpdate updateReceiver: Optional<([UbiquitousDocumentMetadata]) -> Bool> = nil
    ) async throws -> [UbiquitousDocumentMetadata] {
        guard self.usesUbiquitousStorage else {
            throw CryoError.featureNotAvailable(message: "loadUbiquitousDocuments is only available for an iCloud documents adaptor")
        }
        
        let query = ItemQuery(query: makeMetadataQuery())
        let predicate = query.createQueryPredicate(directory: url, filenamePattern: filenamePattern)
        
        return try await query.searchMetadataItems(baseUrl: url, predicate: predicate, onUpdate: updateReceiver)
    }
    
    public func downloadUbiquitousDocuments(at url: URL, filenameMatching filenamePattern: String? = nil) throws -> AsyncThrowingStream<UbiquitousItemUpdate, Error> {
        guard self.usesUbiquitousStorage else {
            throw CryoError.featureNotAvailable(message: "loadUbiquitousDocuments is only available for an iCloud documents adaptor")
        }
        
        let query = ItemQuery(query: makeMetadataQuery())
        let predicate = query.createQueryPredicate(directory: url, filenamePattern: filenamePattern)
        
        return query.downloadUbiqitousFiles(baseUrl: url, fileManager: fileManager, config: config, predicate: predicate)
    }
}

internal final class ItemQuery {
    let query: any UbiquitousMetadataQuerying

    init(query: any UbiquitousMetadataQuerying) { self.query = query }

    func createQueryPredicate(directory: URL, filenamePattern: String? = nil) -> NSPredicate {
        if let filenamePattern {
            return NSPredicate(format: "%K BEGINSWITH[cdw] %@ AND %K LIKE[cwd] %@",
                               NSMetadataItemPathKey, directory.path, NSMetadataItemFSNameKey, filenamePattern)
        }
        return NSPredicate(format: "%K BEGINSWITH[cdw] %@", NSMetadataItemPathKey, directory.path)
    }

    func downloadUbiqitousFiles(baseUrl: URL, fileManager: FileManager, config: CryoConfig,
                               predicate: NSPredicate? = nil,
                               sortDescriptors: [NSSortDescriptor] = [],
                               scopes: [String] = [NSMetadataQueryUbiquitousDocumentsScope]) -> AsyncThrowingStream<DocumentAdaptor.UbiquitousItemUpdate, Error> {
        AsyncThrowingStream { continuation in
            let task = Task { @MainActor in
                defer { query.stop() }
                guard query.start(predicate: predicate ?? NSPredicate(value: true), sortDescriptors: sortDescriptors, scopes: scopes) else {
                    continuation.finish(throwing: CryoError.queryExecutionFailed(query: "metadata", status: -1, message: "starting metadata query failed"))
                    return
                }
                var downloadingItems = Set<URL>()
                do {
                    for await event in query.events {
                        let items: [any UbiquitousMetadataItem]
                        let initial: Bool
                        switch event {
                        case .gathered(let values): items = values; initial = true
                        case .updated(let values): items = values; initial = false
                        }
                        var remaining = Set<URL>()
                        for metadata in items {
                            guard let item = UbiquitousDocumentMetadata(baseUrl: baseUrl, metadataItem: metadata),
                                  initial || downloadingItems.contains(item.fileUrl) else { continue }
                            if item.isDownloaded {
                                continuation.yield(.item(item))
                            } else {
                                if !item.isDownloading {
                                    try fileManager.startDownloadingUbiquitousItem(at: item.fileUrl)
                                }
                                remaining.insert(item.fileUrl)
                            }
                        }
                        downloadingItems = remaining
                        if remaining.isEmpty { break }
                        continuation.yield(.progressUpdate(downloaded: items.count - remaining.count, total: items.count))
                    }
                    continuation.finish()
                } catch { continuation.finish(throwing: error) }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    func searchMetadataItems(baseUrl: URL, predicate: NSPredicate? = nil,
                             sortDescriptors: [NSSortDescriptor] = [],
                             scopes: [String] = [NSMetadataQueryUbiquitousDocumentsScope],
                             onUpdate updateReceiver: Optional<([UbiquitousDocumentMetadata]) -> Bool>) async throws -> [UbiquitousDocumentMetadata] {
        let lifetime = MetadataSearchLifetime()
        return try await withTaskCancellationHandler(operation: {
            try await withCheckedThrowingContinuation { continuation in
                let task = Task { @MainActor in
                    defer { query.stop() }
                    var deliveredInitialResults = false
                    guard query.start(predicate: predicate ?? NSPredicate(value: true), sortDescriptors: sortDescriptors, scopes: scopes) else {
                        continuation.resume(throwing: CryoError.queryExecutionFailed(query: "metadata", status: -1, message: "starting metadata query failed"))
                        return
                    }
                    for await event in query.events {
                        switch event {
                        case .gathered(let items):
                            guard !deliveredInitialResults else { continue }
                            deliveredInitialResults = true
                            continuation.resume(returning: items.compactMap {
                                UbiquitousDocumentMetadata(baseUrl: baseUrl, metadataItem: $0)
                            })
                            if updateReceiver == nil { return }
                        case .updated(let items):
                            let result = items.compactMap { UbiquitousDocumentMetadata(baseUrl: baseUrl, metadataItem: $0) }
                            if updateReceiver?(result) == false {
                                if !deliveredInitialResults { continuation.resume(returning: result) }
                                return
                            }
                        }
                    }
                    if !deliveredInitialResults { continuation.resume(throwing: CancellationError()) }
                }
                lifetime.install(task)
            }
        }, onCancel: { lifetime.cancel() })
    }
}

public struct UbiquitousDocumentMetadata: Codable, Sendable {
    /// The name of the file.
    public let fileName: String
    
    /// The URL of the file.
    public let fileUrl: URL
    
    /// The size of the file in bytes.
    public let fileSize: Int?
    
    /// The content type of the file.
    public let contentType: String?

    /// Whether the file is a placeholder.
    public let isPlaceholder: Bool

    /// Whether the file is currently being downloaded.
    public let isDownloading: Bool

    /// The amount of the file that has been downloaded.
    public let downloadAmount: Double?
    
    /// Whether the item is fully downloaded.
    public let isDownloaded: Bool

    /// Whether the file is a directory.
    public let isDirectory: Bool

    /// Whether the file is being uploaded.
    public let isUploading: Bool

    /// Whether the file has been uploaded.
    public let isUploaded: Bool
    
    internal init? (baseUrl: URL, metadataItem: any UbiquitousMetadataItem) {
        guard let fileName = metadataItem.value(forAttribute: NSMetadataItemFSNameKey) as? String else {
            return nil
        }
        guard
            let fileUrlString = metadataItem.value(forAttribute: NSMetadataItemPathKey) as? String
        else {
            return nil
        }
        let fileUrl = URL(fileURLWithPath: fileUrlString)
        
        self.fileName = fileName
        self.fileUrl = fileUrl
        
        self.fileSize = metadataItem.value(forAttribute: NSMetadataItemFSSizeKey) as? Int
        self.contentType = metadataItem.value(forAttribute: NSMetadataItemContentTypeKey) as? String
        let downloadStatus = metadataItem.value(forAttribute: NSMetadataUbiquitousItemDownloadingStatusKey) as? String
        self.isPlaceholder = downloadStatus != NSMetadataUbiquitousItemDownloadingStatusCurrent
            && downloadStatus != NSMetadataUbiquitousItemDownloadingStatusDownloaded
        self.downloadAmount = metadataItem.value(forAttribute: NSMetadataUbiquitousItemPercentDownloadedKey) as? Double
        self.isDownloading = metadataItem.value(forAttribute: NSMetadataUbiquitousItemIsDownloadingKey) as? Bool ?? false
        self.isDownloaded = downloadStatus == NSMetadataUbiquitousItemDownloadingStatusCurrent
            || downloadStatus == NSMetadataUbiquitousItemDownloadingStatusDownloaded
        
        // Check if it is a directory
        if let contentType = metadataItem.value(forAttribute: NSMetadataItemContentTypeKey) as? String {
            self.isDirectory = (contentType == "public.folder")
        } else {
            self.isDirectory = false
        }
        
        // Check whether the file has been uploaded successfully or saved in the cloud
        let uploaded = metadataItem.value(forAttribute: NSMetadataUbiquitousItemIsUploadedKey) as? Bool ?? false
        let uploading = metadataItem.value(forAttribute: NSMetadataUbiquitousItemIsUploadingKey) as? Bool ?? true
        
        self.isUploading = uploading
        self.isUploaded = uploaded && !uploading
    }
}
