
public enum MacroError {
    /// The type is not a struct.
    case notStruct(type: Any.Type)
}

extension MacroError: Error {
    public var localizedDescription: String {
        switch self {
        case let .notStruct(type):
            return "The type \(type) is not a struct."
        }
    }
}
