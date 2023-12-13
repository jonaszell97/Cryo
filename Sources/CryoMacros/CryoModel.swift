
import SwiftSyntax
import SwiftSyntaxMacros

public struct CryoModelMacro: ExtensionMacro, MemberMacro {
    public static func expansion(of node: AttributeSyntax,
                                 providingMembersOf declaration: some DeclGroupSyntax,
                                 in context: some MacroExpansionContext) throws -> [DeclSyntax] {
        guard let classDecl = declaration.as(ClassDeclSyntax.self) else {
            throw MacroError.notStruct(type: Swift.type(of: declaration))
        }
        
        return [
            createIdProperty(for: classDecl),
            createContextProperty(for: classDecl),
            createTableNameProperty(for: classDecl, node: node),
            createDefaultInitializer(for: classDecl),
        ]
    }
    
    public static func expansion(of node: AttributeSyntax,
                                 attachedTo declaration: some DeclGroupSyntax,
                                 providingExtensionsOf type: some TypeSyntaxProtocol,
                                 conformingTo protocols: [TypeSyntax],
                                 in context: some MacroExpansionContext) throws -> [ExtensionDeclSyntax] {
        guard let classDecl = declaration.as(ClassDeclSyntax.self) else {
            throw MacroError.notStruct(type: Swift.type(of: declaration))
        }
        
        let syntax: DeclSyntax = """
        extension \(type): CryoClassModel {
            \(createSchemaProperty(for: classDecl))
        }
        """
        
        // print(declaration.debugDescription)
        return [
            syntax.as(ExtensionDeclSyntax.self)!,
        ]
    }
}

// MARK: Context

extension CryoModelMacro {
    /// Create the `context` property.
    static func createContextProperty(for declaration: ClassDeclSyntax) -> DeclSyntax {
        """
        let context: CryoContext
        """
    }
    
    /// Create the `id` property.
    static func createIdProperty(for declaration: ClassDeclSyntax) -> DeclSyntax {
        """
        public let id: String
        """
    }
}

// MARK: tableName

extension CryoModelMacro {
    /// Create the `tableName` property.
    static func createTableNameProperty(for declaration: ClassDeclSyntax, node: AttributeSyntax) -> DeclSyntax {
        let customName = getCustomTableName(node: node)
        
        return """
        public static let tableName: String = "\(raw: customName ?? declaration.name.text)"
        """
    }
    
    /// Find a custom table name argument.
    static func getCustomTableName(node: AttributeSyntax) -> String? {
        guard let arguments = node.arguments else {
            return nil
        }
        
        for argument in arguments.children(viewMode: .all) {
            guard let labeledExpr = argument.as(LabeledExprSyntax.self) else {
                continue
            }
            guard let labelSyntax = labeledExpr.label?.tokenKind, case .identifier(let str) = labelSyntax, str == "tableName" else {
                continue
            }
            guard let stringExpr = labeledExpr.expression.as(StringLiteralExprSyntax.self) else {
                continue
            }
            guard stringExpr.segments.count == 1, let first = stringExpr.segments.first else {
                continue
            }
            guard let string = first.as(StringSegmentSyntax.self) else {
                continue
            }
            
            return string.content.text
        }
        
        return nil
    }
}

// MARK: Empty initializer

extension CryoModelMacro {
    /// Create the default `init`.
    static func createDefaultInitializer(for declaration: ClassDeclSyntax) -> DeclSyntax {
        let properties = getPropertyDeclarations(of: declaration.memberBlock)
        let accessModifier = getAccessModifier(for: declaration.modifiers)
        
        var initializerDecls: [ExprSyntax] = []
        for prop in properties {
            if let initializer = prop.initializerExpression {
                initializerDecls.append("self.\(prop.name) \(initializer)")
            }
            else {
                initializerDecls.append("self.\(prop.name) = CryoContext.defaultValue(for: \(prop.type).self)")
            }
        }
        
        return """
        \(raw: accessModifier)init(context: CryoContext) async throws {
            self.id = UUID().uuidString
            self.context = context
            
            \(raw: initializerDecls.map { $0.description }.joined(separator: "\n    ") )
            
            try await context.manage(self)
        }
        """
    }
}

// MARK: Schema

extension CryoModelMacro {
    /// Create the `schema` property.
    static func createSchemaProperty(for declaration: ClassDeclSyntax) -> DeclSyntax {
        let properties = getPropertyDeclarations(of: declaration.memberBlock)
        let accessModifier = getAccessModifier(for: declaration.modifiers)
        
        var columnExpressions: [ExprSyntax] = []
        for property in properties {
            switch property.kind {
            case PropertyKind.persisted:
                columnExpressions.append("""
                let type_\(property.name) = CryoContext.columnType(for: \(property.type).self)
                try columns.append(.value(columnName: "\(property.name)", type: type_\(property.name)) { this in
                        return (this as! \(declaration.name))._\(property.name)
                    })
                """)
            default:
                continue
            }
        }
return """
\(raw: accessModifier)static let schema: CryoSchema = {
    var columns: [CryoSchemaColumn] = []
    \(raw: columnExpressions.map { $0.description }.joined(separator: "\n    ") )

    return CryoSchema(self: Self.self) { try Self(context: $0) }
}()
"""
    }
}
