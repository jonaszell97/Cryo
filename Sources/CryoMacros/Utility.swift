
import SwiftSyntax
import SwiftSyntaxMacros

enum RelationshipType {
    case oneToTone
    case oneToMany
}

enum PropertyKind {
    case persisted
    case transient
    case computed
    case relationship(type: RelationshipType)
}

struct PropertyDeclaration {
    let name: IdentifierPatternSyntax
    let kind: PropertyKind
    let type: TypeSyntax
    var initializerExpression: InitializerClauseSyntax?
}

func getPropertyDeclarations(of declaration: MemberBlockSyntax) -> [PropertyDeclaration] {
    var result = [PropertyDeclaration]()
    for member in declaration.members {
        guard let variableDecl = member.decl.as(VariableDeclSyntax.self) else {
            continue
        }
        
        for binding in variableDecl.bindings {
            guard let identifier = binding.pattern.as(IdentifierPatternSyntax.self) else {
                continue
            }
            guard let type = binding.typeAnnotation else {
                continue
            }
            
            result.append(.init(name: identifier, kind: .persisted,
                                type: type.type, initializerExpression: binding.initializer))
        }
    }
    
    return result
}

func getAccessModifier(for declaration: DeclModifierListSyntax) -> String {
    for modifier in declaration {
        guard case .keyword(let keyword) = modifier.name.tokenKind else {
            continue
        }
        
        if keyword == .public {
            return "public "
        }
    }
    
    return ""
}
