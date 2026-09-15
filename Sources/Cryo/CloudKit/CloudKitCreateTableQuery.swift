
import CloudKit
import Foundation

public final class CloudKitCreateTableQuery<Model: CryoModel> {
    /// The untyped query.
    let untypedQuery: UntypedCloudKitCreateTableQuery
    
    /// Create a CREATE TABLE query.
    internal init(from: Model.Type, database: any CloudKitDatabase, config: CryoConfig?, initializeCloudKitSchema: Bool) throws {
        self.untypedQuery = try .init(for: Model.self, database: database, config: config, initializeCloudKitSchema: initializeCloudKitSchema)
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
    let database: any CloudKitDatabase
    
    /// Whether to initialized the CloudKit schema by inserting a dummy value.
    let initializeCloudKitSchema: Bool
    
    let config: CryoConfig?
    
    /// Create a CREATE TABLE query.
    internal init(for modelType: any CryoModel.Type, database: any CloudKitDatabase, config: CryoConfig?, initializeCloudKitSchema: Bool) throws {
        self.database = database
        self.modelType = modelType
        self.initializeCloudKitSchema = initializeCloudKitSchema
        
        self.config = config
    }
    
    /// The complete query string.
    public var queryString: String { "CREATE TABLE \(modelType.tableName)" }
}

extension UntypedCloudKitCreateTableQuery {
    public typealias Result = Void
    
    public func execute() async throws {
        if !initializeCloudKitSchema {
            return
        }
        
        let id = "_cryo_schema_\(modelType.tableName)"
        let value = try modelType.init(from: EmptyDecoder())
        
        // Remove a dummy left behind by an interrupted earlier initialization.
        _ = try await UntypedCloudKitDeleteQuery(for: modelType, id: id, database: database, config: config).execute()
        config?.log?(.debug, "creating table \(modelType.tableName)")
        do {
            try await UntypedCloudKitInsertQuery(id: id, value: value, replace: true, database: database, config: config).execute()
            config?.log?(.debug, "deleting dummy value for table \(modelType.tableName)")
            _ = try await UntypedCloudKitDeleteQuery(for: modelType, id: id, database: database, config: config).execute()
        } catch {
            _ = try? await UntypedCloudKitDeleteQuery(for: modelType, id: id, database: database, config: config).execute()
            throw error
        }
    }
}
