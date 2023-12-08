
import SwiftSyntax
import SwiftSyntaxMacros

public struct CryoColumnMacro: PeerMacro, AccessorMacro {
    public static func expansion(of node: AttributeSyntax,
                                 providingPeersOf declaration: some DeclSyntaxProtocol,
                                 in context: some MacroExpansionContext) throws -> [DeclSyntax] {
        var result = [DeclSyntax]()
        guard let variableDecl = declaration.as(VariableDeclSyntax.self) else {
            return result
        }
        
        for binding in variableDecl.bindings {
            result.append(createStoredProperty(for: binding))
        }
        
        return result
    }
    
    public static func expansion(of node: AttributeSyntax,
                                 providingAccessorsOf declaration: some DeclSyntaxProtocol,
                                 in context: some MacroExpansionContext) throws -> [AccessorDeclSyntax] {
        var result = [AccessorDeclSyntax]()
        guard let variableDecl = declaration.as(VariableDeclSyntax.self) else {
            return result
        }
        
        for binding in variableDecl.bindings {
            result.append(createGetter(for: binding))
            result.append(createSetter(for: binding))
        }
        
        return result
    }
}

// MARK: Stored property

extension CryoColumnMacro {
    /// Create the stored property.
    static func createStoredProperty(for declaration: PatternBindingSyntax) -> DeclSyntax {
        if let typeAnnotation = declaration.typeAnnotation {
            return """
            var _\(declaration.pattern)\(declaration.typeAnnotation!)
            """
        }
        
        return """
            var _\(declaration.pattern)
            """
    }
}

// MARK: Getters & Setters

extension CryoColumnMacro {
    /// Create the getter.
    static func createGetter(for declaration: PatternBindingSyntax) -> AccessorDeclSyntax {
        """
        get {
            return _\(declaration.pattern)
        }
        """
    }
    
    /// Create the setter.
    static func createSetter(for declaration: PatternBindingSyntax) -> AccessorDeclSyntax {
        """
        set async throws {
            try await context.update(self.id, from: Self.self).set("\(declaration.pattern)", newValue).execute()
            self._\(declaration.pattern) = newValue
            self.objectWillChange.send()
        }
        """
    }
}
