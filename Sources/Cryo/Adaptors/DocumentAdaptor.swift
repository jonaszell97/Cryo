
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
    /// The cryo config.
    public let config: CryoConfig
    
    /// The URL documents should be saved to.
    public let url: URL
    
    /// The file manager instance to use.
    public let fileManager: FileManager
    
    /// The file coordinator.
    public let coordinator: NSFileCoordinator
    
    /// Whether or not this adaptor uses iCloud ubiquitous storage.
    public let usesUbiquitousStorage: Bool
    
    /// Create a document adaptor.
    ///
    /// - Parameters:
    ///   - url: The URL to the directory where data should be stored.
    ///   - fileManager: The file manager instance to use for file operations.
    public init(config: CryoConfig, url: URL, usesUbiquitousStorage: Bool, fileManager: FileManager = .default) {
        self.config = config
        self.url = url
        self.fileManager = fileManager
        self.usesUbiquitousStorage = usesUbiquitousStorage
        self.coordinator = NSFileCoordinator()
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
        
        try? fileManager.createDirectory(at: url, withIntermediateDirectories: false)
        return DocumentAdaptor(config: config, url: url, usesUbiquitousStorage: false, fileManager: fileManager)
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
        
        try? fileManager.createDirectory(at: containerUrl, withIntermediateDirectories: false)
        return DocumentAdaptor(config: config, url: containerUrl, usesUbiquitousStorage: true, fileManager: fileManager)
    }
}

extension DocumentAdaptor: CryoAdaptor, CryoSynchronousAdaptor {
    func documentUrl<Key: CryoKey>(for key: Key) -> URL {
        if #available(iOS 16, macOS 13, *) {
            return self.url.appending(component: key.id)
        }
        else {
            return self.url.appendingPathComponent(key.id)
        }
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
        
        var coordinationError: NSError? = nil
        var writeError: Error? = nil
        
        // Use the coordinationError variable to capture the error information of the coordinate method.
        // If an NSError pointer is not provided, errors occurring during the coordination process will not be caught and handled.
        config.log?(.info, "[DocumentAdaptor.persistSynchronously] starting coordination")
//        coordinator.coordinate(writingItemAt: url, options: [.forDeleting], error: &coordinationError) { url in
            config.log?(.info, "[DocumentAdaptor.persistSynchronously] in coordination callback")
            do {
                if let data {
                    config.log?(.info, "[DocumentAdaptor.persistSynchronously] write \(data.count) bytes")
                    try data.write(to: url)
                }
                else {
                    try self.fileManager.removeItem(at: url)
                }
                
                config.log?(.info, "[DocumentAdaptor.persistSynchronously] completed coordination callback")
            }
            catch {
                writeError = error
                config.log?(.info, "[DocumentAdaptor.persistSynchronously] error in coordination callback: \(error)")
            }
//        }
        
        // Check outside the closure to see if an error occurred
        if let error = writeError {
            config.log?(.info, "[DocumentAdaptor.persistSynchronously] throwing write error: \(error)")
            throw error
        }
        
        // Check if an error occurred during coordination
        if let coordinationError = coordinationError {
            config.log?(.info, "[DocumentAdaptor.persistSynchronously] throwing coordination error: \(coordinationError)")
            throw coordinationError
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
        let documentUrl = self.documentUrl(for: key)
        
        var coordinationError: NSError?
        var readError: Error? = nil
        var data: Data? = nil
        
        coordinator.coordinate(readingItemAt: url, options: [], error: &coordinationError) { url in
            do {
                data = try Data(contentsOf: documentUrl)
            } catch {
                if (error as NSError).code != NSFileReadNoSuchFileError {
                    readError = error
                }
            }
        }
        
        // Check outside the closure to see if an error occurred
        if let error = readError {
            throw error
        }
        
        // Check if an error occurred during reconciliation
        if let coordinationError = coordinationError {
            throw coordinationError
        }
        
        guard let data else {
            return nil
        }
        
        return try JSONDecoder().decode(Key.Value.self, from: data)
    }
    
    public func removeAll() async throws {
        let urls = try FileManager.default.contentsOfDirectory(at: self.url, includingPropertiesForKeys: nil)
        for url in urls {
            try await self.persist(nil, url: url)
        }
    }
    
    public func removeAllSynchronously() throws {
        let urls = try FileManager.default.contentsOfDirectory(at: self.url, includingPropertiesForKeys: nil)
        for url in urls {
            try self.persistSynchronously(nil, url: url)
        }
    }
    
    public func removeAll(matching condition: (URL) -> Bool) async throws {
        let urls = try FileManager.default.contentsOfDirectory(at: self.url, includingPropertiesForKeys: nil)
        for url in urls {
            guard condition(url) else { continue }
            try await self.persist(nil, url: url)
        }
    }
    
    /// List all instances available in this adaptor.
    ///
    /// - Note: This method is not available in all adaptors.
    public func listInstanceKeys() async throws -> [String]? {
        try FileManager.default.contentsOfDirectory(
            at: self.url, includingPropertiesForKeys: nil
        ).map { $0.lastPathComponent }
    }
    
    /// List all instances available in this adaptor.
    ///
    /// - Note: This method is not available in all adaptors.
    public func listInstanceKeysSynchronously() throws -> [String]? {
        try FileManager.default.contentsOfDirectory(
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
        
        let query = ItemQuery()
        let predicate = query.createQueryPredicate(directory: url, filenamePattern: filenamePattern)
        
        return try await query.searchMetadataItems(baseUrl: url, predicate: predicate, onUpdate: updateReceiver)
    }
    
    public func downloadUbiquitousDocuments(at url: URL, filenameMatching filenamePattern: String? = nil) throws -> AsyncThrowingStream<UbiquitousItemUpdate, Error> {
        guard self.usesUbiquitousStorage else {
            throw CryoError.featureNotAvailable(message: "loadUbiquitousDocuments is only available for an iCloud documents adaptor")
        }
        
        let query = ItemQuery()
        let predicate = query.createQueryPredicate(directory: url, filenamePattern: filenamePattern)
        
        return query.downloadUbiqitousFiles(baseUrl: url, fileManager: fileManager, config: config, predicate: predicate)
    }
}

fileprivate class ItemQuery {
    /// The metadata query object.
    let query: NSMetadataQuery
    
    /// The queue on which to execute operations.
    let queue: OperationQueue
    
    /// Create a new item query.
    init (queue: OperationQueue = .main) {
        self.query = NSMetadataQuery()
        self.queue = queue
    }
    
    /// Create a new predicate for a metadat query.
    /// 
    /// - Parameters:
    ///  - directory: The directory to search in.
    ///  - recursive: Whether to search recursively.
    ///  - filenamePattern: The filename pattern to match.
    /// - Returns: A new predicate.
    func createQueryPredicate(directory: URL, filenamePattern: String? = nil) -> NSPredicate {
        if let filenamePattern {
            return NSPredicate(format: "%K BEGINSWITH[cdw] %@ AND %K LIKE[cwd] %@",
                               NSMetadataItemPathKey, directory.path, NSMetadataItemFSNameKey, filenamePattern)
        }
        else {
            return NSPredicate(format: "%K BEGINSWITH[cdw] %@", NSMetadataItemPathKey, directory.path)
        }
    }
    
    /// Search for metadata items.
    ///
    /// - Parameters:
    ///  - predicate: The predicate to use for the search.
    ///  - sortDescriptors: The sort descriptors to use for the search.
    ///  - scopes: The search scopes to use for the search.
    /// - Returns: An async stream of metadata items.
    func downloadUbiqitousFiles(baseUrl: URL, fileManager: FileManager, config: CryoConfig,
                                predicate: NSPredicate? = nil,
                                sortDescriptors: [NSSortDescriptor] = [],
                                scopes: [String] = [NSMetadataQueryUbiquitousDocumentsScope]) -> AsyncThrowingStream<DocumentAdaptor.UbiquitousItemUpdate, Error> {
        // Configure query
        query.searchScopes = [NSMetadataQueryUbiquitousDocumentsScope]
        query.sortDescriptors = []
        query.predicate = predicate ?? NSPredicate(value: true)
        
        let queryId = "\(ObjectIdentifier(self))".prefix(5)
        let log: (String) -> Void = { message in
            config.log?(.debug, "[ItemQuery \(queryId)] \(message)")
        }
        
        return AsyncThrowingStream { continuation in
            var downloadingItems = Set<URL>()
            log("setting up metadata query")
            
            // Set up handler for initial results
            NotificationCenter.default.addObserver(
                forName: .NSMetadataQueryDidFinishGathering,
                object: query,
                queue: queue
            ) { _ in
                let fileManager = FileManager()
                log("received \(self.query.results.count) initial items")
                
                for result in self.query.results {
                    guard let metadataItem = result as? NSMetadataItem else {
                        continue
                    }
                    
                    guard let item = UbiquitousDocumentMetadata(baseUrl: baseUrl, metadataItem: metadataItem) else {
                        continue
                    }
                    
                    if item.isDownloaded {
                        continuation.yield(.item(item))
                        continue
                    }
                    
                    // If the download is not started, start it
                    if !item.isDownloading {
                        do {
                            try fileManager.startDownloadingUbiquitousItem(at: item.fileUrl)
                        }
                        catch {
                            continuation.finish(throwing: error)
                        }
                    }
                    
                    downloadingItems.insert(item.fileUrl)
                }
                
                log("\(downloadingItems.count) files need to be downloaded")
                
                // Remove observer for initial results
                NotificationCenter.default.removeObserver(self, name: .NSMetadataQueryDidFinishGathering, object: self.query)
                
                // If no values are downloading, finish
                if downloadingItems.isEmpty {
                    continuation.finish()
                }
                else {
                    // Send an update about the download progress
                    continuation.yield(.progressUpdate(downloaded: self.query.results.count - downloadingItems.count, total: self.query.results.count))
                }
            }
            
            NotificationCenter.default.addObserver(
                forName: .NSMetadataQueryDidUpdate,
                object: query,
                queue: queue
            ) { _ in
                let fileManager = FileManager()
                log("received update with \(self.query.results) items")
                
                var newDownloadingItems = Set<URL>()
                for result in self.query.results {
                    guard let metadataItem = result as? NSMetadataItem else {
                        continue
                    }
                    
                    guard let item = UbiquitousDocumentMetadata(baseUrl: baseUrl, metadataItem: metadataItem) else {
                        continue
                    }
                    
                    guard downloadingItems.contains(item.fileUrl) else {
                        continue
                    }
                    
                    if item.isDownloaded {
                        continuation.yield(.item(item))
                        continue
                    }
                    
                    // If the download is not started, start it
                    if !item.isDownloading {
                        do {
                            try fileManager.startDownloadingUbiquitousItem(at: item.fileUrl)
                        }
                        catch {
                            continuation.finish(throwing: error)
                        }
                    }
                    
                    newDownloadingItems.insert(item.fileUrl)
                }
                
                log("\(newDownloadingItems.count) files still need to be downloaded")
                downloadingItems = newDownloadingItems
                
                if newDownloadingItems.isEmpty {
                    continuation.finish()
                }
                else {
                    // Send an update about the download progress
                    continuation.yield(.progressUpdate(downloaded: self.query.results.count - downloadingItems.count, total: self.query.results.count))
                }
            }
            
            continuation.onTermination = { termination in
                NotificationCenter.default.removeObserver(self, name: .NSMetadataQueryDidUpdate, object: self.query)
                self.query.stop()
            }
            
            // Start the query
            query.operationQueue = queue
            queue.addOperation {
                Task { @MainActor in
                    let started = self.query.start()
                    if !started {
                        continuation.finish(throwing: CryoError.queryExecutionFailed(query: self.query.description, status: -1, message: "starting metadata query failed"))
                    }
                }
            }
        }
    }
    
    /// Search for metadata items.
    /// 
    /// - Parameters:
    ///  - predicate: The predicate to use for the search.
    ///  - sortDescriptors: The sort descriptors to use for the search.
    ///  - scopes: The search scopes to use for the search.
    /// - Returns: An async stream of metadata items.
    func searchMetadataItems(baseUrl: URL,
                             predicate: NSPredicate? = nil,
                             sortDescriptors: [NSSortDescriptor] = [],
                             scopes: [String] = [NSMetadataQueryUbiquitousDocumentsScope],
                             onUpdate updateReceiver: Optional<([UbiquitousDocumentMetadata]) -> Bool>) async throws -> [UbiquitousDocumentMetadata] {
        // Configure query
        query.searchScopes = scopes
        query.sortDescriptors = sortDescriptors
        query.predicate = predicate ?? NSPredicate(value: true)
        
        // Set up handler for updates
        if let updateReceiver {
            NotificationCenter.default.addObserver(
                forName: .NSMetadataQueryDidUpdate,
                object: query,
                queue: queue
            ) { _ in
                let result = self.query.results.compactMap { item -> UbiquitousDocumentMetadata? in
                    guard let metadataItem = item as? NSMetadataItem else {
                        return nil
                    }
                    
                    return UbiquitousDocumentMetadata(baseUrl: baseUrl, metadataItem: metadataItem)
                }
                
                let continueReceivingUpdates = updateReceiver(result)
                if !continueReceivingUpdates {
                    NotificationCenter.default.removeObserver(self, name: .NSMetadataQueryDidUpdate, object: self.query)
                    self.query.stop()
                }
            }
        }
        
        // Create and return the stream
        return try await withCheckedThrowingContinuation { continuation in
            // Set up handler for first results
            NotificationCenter.default.addObserver(
                forName: .NSMetadataQueryDidFinishGathering,
                object: query,
                queue: queue
            ) { _ in
                let result = self.query.results.compactMap { item -> UbiquitousDocumentMetadata? in
                    guard let metadataItem = item as? NSMetadataItem else {
                        return nil
                    }

                    return UbiquitousDocumentMetadata(baseUrl: baseUrl, metadataItem: metadataItem)
                }

                NotificationCenter.default.removeObserver(self, name: .NSMetadataQueryDidFinishGathering, object: self.query)
                continuation.resume(returning: result)
            }
            
            // Start the query
            query.operationQueue = queue
            queue.addOperation {
                let started = self.query.start()
                if !started {
                    continuation.resume(throwing:
                        CryoError.queryExecutionFailed(query: self.query.description, status: -1, message: "starting metadata query failed"))
                }
            }
        }
    }
}

public struct UbiquitousDocumentMetadata: Sendable {
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
    public var isDownloaded: Bool { (downloadAmount ?? 0) >= 100 }

    /// Whether the file is a directory.
    public let isDirectory: Bool

    /// Whether the file is being uploaded.
    public let isUploading: Bool

    /// Whether the file has been uploaded.
    public let isUploaded: Bool
    
    fileprivate init? (baseUrl: URL, metadataItem: NSMetadataItem) {
        guard let fileName = metadataItem.value(forAttribute: NSMetadataItemFSNameKey) as? String else {
            return nil
        }
        guard let fileUrl = metadataItem.value(forAttribute: NSMetadataItemPathKey) as? URL else {
            return nil
        }
        
        self.fileName = fileName
        self.fileUrl = fileUrl
        
        self.fileSize = metadataItem.value(forAttribute: NSMetadataItemFSSizeKey) as? Int
        self.contentType = metadataItem.value(forAttribute: NSMetadataItemContentTypeKey) as? String
        self.isPlaceholder = metadataItem.value(forAttribute: NSMetadataUbiquitousItemDownloadingStatusKey) as? Bool ?? false
        self.downloadAmount = metadataItem.value(forAttribute: NSMetadataUbiquitousItemPercentDownloadedKey) as? Double
        
        let downloadStatus = metadataItem.value(forAttribute: NSMetadataUbiquitousItemIsDownloadingKey) as? String
        self.isDownloading = downloadStatus == NSMetadataUbiquitousItemDownloadingStatusCurrent
        
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
