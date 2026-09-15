import Foundation

final class FakeUbiquitousKeyValueStore: NSUbiquitousKeyValueStore {
    private var values: [String: Any] = [:]
    var synchronizeResult = true
    override var dictionaryRepresentation: [String: Any] { values }
    override func object(forKey key: String) -> Any? { values[key] }
    override func set(_ value: Any?, forKey key: String) { values[key] = value }
    override func removeObject(forKey key: String) { values.removeValue(forKey: key) }
    override func synchronize() -> Bool { synchronizeResult }
    override func string(forKey key: String) -> String? { values[key] as? String }
    override func data(forKey key: String) -> Data? { values[key] as? Data }
    override func longLong(forKey key: String) -> Int64 { (values[key] as? NSNumber)?.int64Value ?? 0 }
    override func double(forKey key: String) -> Double { (values[key] as? NSNumber)?.doubleValue ?? 0 }
    override func bool(forKey key: String) -> Bool { (values[key] as? NSNumber)?.boolValue ?? false }
    override func set(_ value: Int64, forKey key: String) { values[key] = NSNumber(value: value) }
    override func set(_ value: Double, forKey key: String) { values[key] = NSNumber(value: value) }
    override func set(_ value: Bool, forKey key: String) { values[key] = NSNumber(value: value) }
}
