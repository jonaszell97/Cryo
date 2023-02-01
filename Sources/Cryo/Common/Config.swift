
import Foundation
import os

public struct CryoConfig {
    /// Used for logging non-critical errors and other messages.
    public var log: Optional<(OSLogType, String) -> Void> = nil
    
    /// Public initalizer.
    public init(log: Optional<(OSLogType, String) -> Void> = nil) {
        self.log = log
    }
}
