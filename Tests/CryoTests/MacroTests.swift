#if canImport(CryoMacros)

import CryoMacros
import SwiftSyntaxMacros
import SwiftSyntaxMacrosTestSupport
import XCTest

let testMacros: [String: Macro.Type] = [
    "CryoModel": CryoModelMacro.self,
    "CryoColumn": CryoColumnMacro.self,
]

final class MacroTests: XCTestCase {
    // Cryo Model
    
    func testCryoModelMacro() throws {
        assertMacroExpansion(
            """
            @CryoModel public class MyModel {
                @CryoColumn var x: Int = 3
                @CryoColumn var y: String
            }
            """,
            expandedSource: """
            public struct MyModel {
                var x: Int
                var y: String
            }
            
            extension MyModel: CryoModel {
                static let tableName: String = "MyModel"
            }
            """,
            macros: testMacros
        )
    }
    
//    func testCryoModelMacroCustomTableName() throws {
//        assertMacroExpansion(
//            """
//            @CryoModel(tableName: "CustomTableName") public struct MyModel { var x: Int; var y: String }
//            """,
//            expandedSource: """
//            public struct MyModel { var x: Int; var y: String }
//            
//            extension MyModel: CryoModel {
//                static let tableName: String = "CustomTableName"
//            }
//            """,
//            macros: testMacros
//        )
//    }
}

#endif
