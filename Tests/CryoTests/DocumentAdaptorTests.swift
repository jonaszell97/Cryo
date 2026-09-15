import XCTest
@testable import Cryo

final class DocumentAdaptorTests: CryoTestCase {
    func testMissingRemoveAndInvalidIdentifiers() async throws {
        let missing = CryoNamedKey(id: "missing", for: Int.self)
        try await environment.documents.remove(with: missing)

        for id in ["../escape", "folder/value", "folder\\value"] {
            do {
                try await environment.documents.persist(1, for: CryoNamedKey(id: id, for: Int.self))
                XCTFail("Expected invalid identifier: \(id)")
            } catch let error as CocoaError {
                XCTAssertEqual(error.code, .fileWriteInvalidFileName)
            }
        }
    }

    func testRemoveAllRequiresOwnershipAndKeepsDirectories() async throws {
        let unscoped = DocumentAdaptor(config: .init(), url: environment.root,
                                       usesUbiquitousStorage: false)
        await XCTAssertThrowsErrorAsync { try await unscoped.removeAll() }

        let nested = environment.documents.url.appendingPathComponent("nested")
        try FileManager.default.createDirectory(at: nested, withIntermediateDirectories: true)
        let key = CryoNamedKey(id: "value", for: Int.self)
        try await environment.documents.persist(1, for: key)
        try await environment.documents.removeAll()
        XCTAssertTrue(FileManager.default.fileExists(atPath: nested.path))
        XCTAssertNil(try environment.documents.loadSynchronously(with: key))
    }

    func testUbiquitousIOUsesCoordinator() async throws {
        var adaptor = DocumentAdaptor(config: .init(), url: environment.root,
                                      usesUbiquitousStorage: true, ownsDirectory: true)
        var accesses: [Bool] = []
        adaptor.coordinate = { url, writing, accessor in
            accesses.append(writing)
            accessor(url)
        }
        let key = CryoNamedKey(id: "coordinated", for: Int.self)
        try adaptor.persistSynchronously(7, for: key)
        XCTAssertEqual(try adaptor.loadSynchronously(with: key), 7)
        XCTAssertEqual(accesses, [true, false])
    }

    func testMetadataUsesStatusTypesAndFileURL() {
        let path = environment.root.appendingPathComponent("item").path
        let metadata = UbiquitousDocumentMetadata(baseUrl: environment.root, metadataItem: FakeMetadataItem(attributes: [
            NSMetadataItemFSNameKey: "item",
            NSMetadataItemPathKey: path,
            NSMetadataUbiquitousItemDownloadingStatusKey: NSMetadataUbiquitousItemDownloadingStatusCurrent,
            NSMetadataUbiquitousItemIsDownloadingKey: false
        ]))
        XCTAssertEqual(metadata?.fileUrl.path, path)
        XCTAssertEqual(metadata?.isDownloaded, true)
        XCTAssertEqual(metadata?.isDownloading, false)
        XCTAssertEqual(metadata?.isPlaceholder, false)
    }
}

private extension XCTestCase {
    func XCTAssertThrowsErrorAsync(_ expression: () async throws -> Void,
                                   file: StaticString = #filePath, line: UInt = #line) async {
        do {
            try await expression()
            XCTFail("Expected error", file: file, line: line)
        } catch { }
    }
}
