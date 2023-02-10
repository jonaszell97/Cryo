
import Foundation
import os

/// Shared configuration for `Cryo` classes.
public struct CryoConfig {
    /// Used for logging non-critical errors and other messages.
    public var log: Optional<(OSLogType, String) -> Void> = nil
    
    /// Create a configuration instance.
    ///
    /// - Parameter log: Used for logging non-critical errors and other messages.
    public init(log: Optional<(OSLogType, String) -> Void> = nil) {
        self.log = log
    }
}
