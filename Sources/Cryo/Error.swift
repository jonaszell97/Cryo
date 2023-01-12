
import Foundation

public enum CryoError: Error {
    /// A value cannot be persisted with the given adaptor.
    case cannotPersistValue(valueType: Any.Type, adaptorType: Any.Type)
}
