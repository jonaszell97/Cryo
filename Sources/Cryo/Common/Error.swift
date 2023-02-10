
import Foundation

/// Common type for errors thrown in `Cryo`.
public enum CryoError: Error {
    /// A value cannot be persisted with the given adaptor.
    case cannotPersistValue(valueType: Any.Type, adaptorType: Any.Type)
}
