
@attached(extension, conformances: CryoModel, names: arbitrary)
public macro CryoClassModel(tableName: String? = nil) = #externalMacro(module: "CryoMacros", type: "CryoModelMacro")


@attached(peer, names: arbitrary)
@attached(accessor, names: arbitrary)
public macro CryoClassColumn() = #externalMacro(module: "CryoMacros", type: "CryoColumnMacro")
