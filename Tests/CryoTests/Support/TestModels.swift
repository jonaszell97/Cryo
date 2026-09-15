import Foundation
@testable import Cryo

enum CloudKitTestEnum: Int, CryoColumnIntValue, Hashable {
    case zero = 0
    case a = 300, b = 400, c = 500
    
    /// A default value for this type.
    static var defaultValue: Self { .zero }
}

struct CloudKitTestModel: CryoModel {
    @CryoColumn var id: String = UUID().uuidString
    @CryoColumn var x: Int16 = 0
    @CryoColumn var y: String = ""
    @CryoColumn var z: CloudKitTestEnum = .c
    
    @CryoAsset var w: URL
    
    @CryoColumn var a: [CloudKitTestEnum] = [.a, .b, .c]
    @CryoColumn var b: Int? = nil
    @CryoColumn var c: [String: Int] = ["A": 1, "B": 2]
}

extension CloudKitTestModel: Hashable {
    static func ==(lhs: CloudKitTestModel, rhs: CloudKitTestModel) -> Bool {
        guard lhs.x == rhs.x && lhs.y == rhs.y && lhs.z == rhs.z else {
            return false
        }
        guard lhs.a == rhs.a && lhs.b == rhs.b && lhs.c == rhs.c else {
            return false
        }
        
        guard let data1 = try? Data(contentsOf: lhs.w), let data2 = try? Data(contentsOf: rhs.w) else {
            return false
        }
        
        return data1 == data2
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(x)
        hasher.combine(y)
        hasher.combine(z)
        hasher.combine(a)
        hasher.combine(b)
        hasher.combine(c)
        
        if let data = try? Data(contentsOf: w) {
            hasher.combine(data)
        }
    }
}


struct SeamTestModel: CryoModel {
    @CryoColumn var id: String = "record"
    @CryoColumn var value: String = "value"
}
