
import CloudKit
import Foundation

public final class CloudKitCreateTableQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedCloudKitCreateTableQuery
    
    /// Create a CREATE TABLE query.
    internal init(from: Model.Type, database: CKDatabase, config: CryoConfig?) throws {
        self.untypedQuery = try .init(for: Model.self, database: database, config: config)
    }
}

extension CloudKitCreateTableQuery: CryoCreateTableQuery {
    public var queryString: String {
        untypedQuery.queryString
    }
    
    public func execute() async throws {
        try await untypedQuery.execute()
    }
}

internal class UntypedCloudKitCreateTableQuery {
    /// The model type.
    let modelType: any CryoModel.Type
    
    /// The CloudKit database.
    let database: CKDatabase
    
    #if DEBUG
    let config: CryoConfig?
    #endif
    
    /// Create a CREATE TABLE query.
    internal init(for modelType: any CryoModel.Type, database: CKDatabase, config: CryoConfig?) throws {
        self.database = database
        self.modelType = modelType
        
        #if DEBUG
        self.config = config
        #endif
    }
    
    /// The complete query string.
    public var queryString: String { "CREATE TABLE \(modelType.tableName)" }
}

extension UntypedCloudKitCreateTableQuery {
    public typealias Result = Void
    
    public func execute() async throws {
        #if DEBUG
        let id = UUID().uuidString
        let value = try! modelType.init(from: EmptyDecoder())
        
        config?.log?(.debug, "creating table \(modelType.tableName)")
        try await UntypedCloudKitInsertQuery(id: id, value: value, replace: false, database: database, config: config).execute()
        
        config?.log?(.debug, "deleting dummy value for table \(modelType.tableName)")
        try await UntypedCloudKitDeleteQuery(for: modelType, id: id, database: database, config: config).execute()
        #endif
    }
}
