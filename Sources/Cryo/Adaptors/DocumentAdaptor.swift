
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
    /// The URL documents should be saved to.
    let url: URL
    
    /// The file manager instance to use.
    let fileManager: FileManager
    
    /// Create a document adaptor.
    ///
    /// - Parameters:
    ///   - url: The URL to the directory where data should be stored.
    ///   - fileManager: The file manager instance to use for file operations.
    public init(url: URL, fileManager: FileManager = .default) {
        self.url = url
        self.fileManager = fileManager
    }
    
    /// The shared local document adaptor.
    public static let sharedLocal: DocumentAdaptor = .local()
    
    /// Create a local document adaptor.
    ///
    /// - Parameter fileManager: The file manager instance to use for file operations.
    /// - Returns: A document adaptor using the local documents URL.
    public static func local(fileManager: FileManager = .default) -> DocumentAdaptor {
        let documentDirectory = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)[0]
        
        let url = URL(fileURLWithPath: documentDirectory).appendingPathComponent(".cryo")
        try? fileManager.createDirectory(at: url, withIntermediateDirectories: false)
        
        return DocumentAdaptor(url: url, fileManager: fileManager)
    }
    
    /// Create an iCloud based document adaptor.
    ///
    /// - Parameter fileManager: The file manager instance to use for file operations.
    /// - Returns: A document adaptor using the iCloud documents URL, or `nil` if iCloud is not available.
    public static func cloud(fileManager: FileManager = .default) async -> DocumentAdaptor? {
        guard FileManager.default.ubiquityIdentityToken != nil else {
            return nil
        }
        
        let containerUrl: URL? = await withCheckedContinuation { continuation in
            Task.detached {
                guard let containerUrl = FileManager.default.url(forUbiquityContainerIdentifier: nil)?
                        .appendingPathComponent("Documents")
                        .appendingPathComponent(".cryo")
                else {
                    continuation.resume(returning: nil)
                    return
                }
                
                continuation.resume(returning: containerUrl)
            }
        }
        
        guard let containerUrl else {
            return nil
        }
        
        try? fileManager.createDirectory(at: containerUrl, withIntermediateDirectories: false)
        return DocumentAdaptor(url: containerUrl, fileManager: fileManager)
    }
}

extension DocumentAdaptor: CryoAdaptor {
    func documentUrl<Key: CryoKey>(for key: Key) -> URL {
        if #available(iOS 16, macOS 13, *) {
            return self.url.appending(component: key.id)
        }
        else {
            return self.url.appendingPathComponent(key.id)
        }
    }
    
    public func persist<Key: CryoKey>(_ value: Key.Value?, for key: Key) async throws {
        return try await withCheckedThrowingContinuation { continuation in
            do {
                let documentUrl = self.documentUrl(for: key)
                if let value {
                    let data = try JSONEncoder().encode(value)
                    try data.write(to: documentUrl)
                }
                else {
                    try self.fileManager.removeItem(at: documentUrl)
                }
                
                continuation.resume()
            }
            catch {
                continuation.resume(throwing: error)
            }
        }
    }
    
    public func load<Key: CryoKey>(with key: Key) async throws -> Key.Value? {
        return try await withCheckedThrowingContinuation { continuation in
            Task.detached {
                do {
                    let value = try self.loadSynchronously(with: key)
                    continuation.resume(returning: value)
                }
                catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }
    
    public func loadSynchronously<Key: CryoKey>(with key: Key) throws -> Key.Value? {
        do {
            let documentUrl = self.documentUrl(for: key)
            let data = try Data(contentsOf: documentUrl)
            let value = try JSONDecoder().decode(Key.Value.self, from: data)
            
            return value
        }
        catch {
            if (error as NSError).code == NSFileReadNoSuchFileError {
                return nil
            }
            
            throw error
        }
    }
    
    public func removeAll() async throws {
        return try await withCheckedThrowingContinuation { continuation in
            do {
                let urls = try FileManager.default.contentsOfDirectory(at: self.url, includingPropertiesForKeys: nil)
                for url in urls {
                    try self.fileManager.removeItem(at: url)
                }
                
                continuation.resume()
            }
            catch {
                continuation.resume(throwing: error)
            }
        }
    }
    
    public func removeAll(matching condition: (URL) -> Bool) async throws {
        return try await withCheckedThrowingContinuation { continuation in
            do {
                let urls = try FileManager.default.contentsOfDirectory(at: self.url, includingPropertiesForKeys: nil)
                for url in urls {
                    guard condition(url) else { continue }
                    try self.fileManager.removeItem(at: url)
                }
                
                continuation.resume()
            }
            catch {
                continuation.resume(throwing: error)
            }
        }
    }
}
