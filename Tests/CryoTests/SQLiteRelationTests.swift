
@testable import Cryo
import XCTest

final class CryoSQLiteRelationTests: XCTestCase {
    private var databaseUrl: URL? = nil
    
    override func setUp() {
        super.setUp()
        
        self.databaseUrl = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0].appendingPathComponent("_cryo_test.db")
        
        do { try FileManager.default.removeItem(at: self.databaseUrl!) } catch { }
        FileManager.default.createFile(atPath: self.databaseUrl!.absoluteString, contents: nil)
    }
    
    func testOneToOneRelation() async throws {
        struct ModelA: CryoModel {
            @CryoColumn var id: String
            @CryoColumn var x: Int
            @CryoColumn var y: String
        }
        
        struct ModelB: CryoModel {
            @CryoColumn var id: String
            @CryoColumn var x: Int
            @CryoOneToOne var buddy: ModelA
        }
        
        let adaptor = try SQLiteAdaptor(databaseUrl: self.databaseUrl!, config: CryoConfig { print("[\($0)] \($1)") })
        try await adaptor.enableForeignKeys()
        try await adaptor.createTable(for: ModelA.self).execute()
        try await adaptor.createTable(for: ModelB.self).execute()
        
        let a = ModelA(id: "1", x: 12, y: "hi")
        let b = ModelB(id: "2", x: 31, buddy: a)
        
        // Check foreign key constraint error
        do {
            try await adaptor.insert(b).execute()
            XCTAssert(false, "should throw an error")
        }
        catch let e as CryoError {
            guard case .foreignKeyConstraintFailed = e else {
                XCTAssert(false, "expected error to be foreignKeyConstraintFailed")
                return
            }
        }
        
        // Check valid insert
        try await adaptor.insert(a).execute()
        try await adaptor.insert(b).execute()
        
        // Check loading
        let loadedB = try await adaptor.select(id: b.id, from: ModelB.self).execute().first
        XCTAssertEqual(loadedB?.x, b.x)
        XCTAssertEqual(loadedB?.buddy.x, a.x)
        XCTAssertEqual(loadedB?.buddy.y, a.y)
        
        // Check deletion error
        do {
            try await adaptor.delete(id: a.id, from: ModelA.self).execute()
            XCTAssert(false, "should throw an error")
        }
        catch let e as CryoError {
            guard case .foreignKeyConstraintFailed = e else {
                XCTAssert(false, "expected error to be foreignKeyConstraintFailed")
                return
            }
        }
        
        // Check correct deletion
        try await adaptor.delete(id: b.id, from: ModelB.self).execute()
        try await adaptor.delete(id: a.id, from: ModelA.self).execute()
    }
}
