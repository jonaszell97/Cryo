
import Foundation
import os

public struct CryoConfig {
    /// Used for logging non-critical errors and other messages.
    public var log: Optional<(OSLogType, String) -> Void> = nil
}
