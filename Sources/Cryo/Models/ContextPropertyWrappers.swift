//
//import Combine
//import SwiftUI
//
//public struct
//
//@MainActor @propertyWrapper
//public struct CryoFetchRequest<Model: CryoClassModel>: DynamicProperty {
//    /// The context object.
//    @Environment(\.cryoContext) var cryoContext
//    
//    /// This state object is responsible for reacting to changes in the injected view model.
//    @StateObject private var core = Core()
//    
//    /// The query result.
//    ///
//    /// This property provides primary access to the value's data. However, you
//    /// don't access `wrappedValue` directly. Instead, you use the property
//    /// variable created with the `@Injected` attribute.
//    @MainActor public var wrappedValue: Model {
//        core.object!
//    }
//    
//    /// A projection of the observed object that creates bindings to its
//    /// properties using dynamic member lookup.
//    ///
//    /// Use the projected value to pass a binding value down a view hierarchy.
//    /// To get the `projectedValue`, prefix the property variable with `$`.
//    @MainActor public var projectedValue: Self.Wrapper {
//        Wrapper(baseObject: wrappedValue)
//    }
//    
//    public nonisolated func update() {
//        Task { @MainActor in
//            core.update(registry: self.dependencyRegistry!)
//        }
//    }
//    
//    /// Create an injected dependency wrapper.
//    public init() {
//        
//    }
//    
//    /// Publishes changes to the underlying observable object.
//    /// Mostly taken from https://github.com/groue/GRDBQuery/blob/main/Sources/GRDBQuery/EnvironmentStateObject.swift
//    @MainActor private class Core: ObservableObject {
//        let objectWillChange = PassthroughSubject<Model.ObjectWillChangePublisher.Output, Never>()
//        
//        var cancellable: AnyCancellable?
//        var object: Model?
//        
//        func update(registry: DependencyRegistry) {
//            guard object == nil else {
//                return
//            }
//            
//            // Load the object from the registry
//            let object = registry.get(ObjectType.self)!
//            self.object = object
//            
//            // Pass through all object changes
//            var isUpdating = true
//            cancellable = object.objectWillChange.sink { [weak self] value in
//                guard let self = self else {
//                    return
//                }
//                
//                if !isUpdating {
//                    self.objectWillChange.send(value)
//                }
//            }
//            
//            isUpdating = false
//        }
//    }
//    
//    /// A wrapper of the underlying observable object that can create bindings to
//    /// its properties using dynamic member lookup.
//    @dynamicMemberLookup @frozen public struct Wrapper {
//        /// The object this wrapper is for.
//        let baseObject: Model
//        
//        /// Create a wrapper struct.
//        init(baseObject: Model) {
//            self.baseObject = baseObject
//        }
//        
//        /// Returns a binding to the resulting value of a given key path.
//        ///
//        /// - Parameter keyPath  : A key path to a specific resulting value.
//        ///
//        /// - Returns: A new binding.
//        public subscript<Subject>(dynamicMember keyPath: ReferenceWritableKeyPath<Model, Subject>) -> Binding<Subject> {
//            .init(get: {
//                baseObject[keyPath: keyPath]
//            }, set: {
//                baseObject[keyPath: keyPath] = $0
//            })
//        }
//    }
//}
//
//struct CryoContextEnvironmentKey: EnvironmentKey {
//    static let defaultValue: CryoContext? = nil
//}
//
//public extension EnvironmentValues {
//    var cryoContext: CryoContext? {
//        get {
//            return self[CryoContextEnvironmentKey.self]
//        }
//        set {
//            self[CryoContextEnvironmentKey.self] = newValue
//        }
//    }
//}
