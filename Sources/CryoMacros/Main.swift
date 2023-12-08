
import SwiftCompilerPlugin
import SwiftSyntaxMacros

@main
struct macrotestPlugin: CompilerPlugin {
    let providingMacros: [Macro.Type] = [
        CryoModelMacro.self,
        CryoColumnMacro.self,
    ]
}
