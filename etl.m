let
    Source = Oracle.Database("projetoimpacta_high", [HierarchicalNavigation=true]),
    ADMIN = Source{[Schema="ADMIN"]}[Data],
    DATA_TAB1 = ADMIN{[Name="DATA_TAB"]}[Data],
    #"Changed Type" = Table.TransformColumnTypes(DATA_TAB1,{{"DATADOCUMENTO", type date}, {"VALORDOCUMENTO", type number}, {"ANO", type date}})
in
    #"Changed Type"