
@attached(member, names: arbitrary)
@attached(extension, conformances: CryoClassModel, CryoModel, names: arbitrary)
public macro CryoClassModel(tableName: String? = nil) = #externalMacro(module: "CryoMacros", type: "CryoModelMacro")

@attached(accessor, names: arbitrary)
@attached(peer, names: arbitrary)
public macro CryoClassColumn() = #externalMacro(module: "CryoMacros", type: "CryoColumnMacro")
