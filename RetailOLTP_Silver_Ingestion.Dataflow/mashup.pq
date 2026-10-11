[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"Bronze_Layer Categories" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.Categories", ItemKind = "Table"]}[Data],
  #"Elegir columnas" = Table.SelectColumns(#"Navegación 2", {"CategoryID", "CategoryName"}),
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Elegir columnas", {{"CategoryName", each Text.Proper(_), type nullable text}}),
  #"Texto recortado" = Table.TransformColumns(#"Mayúsculas aplicadas en cada palabra", {{"CategoryName", each Text.Trim(_), type nullable text}}),
  #"Filas agrupadas" = Table.Group(#"Texto recortado", {"CategoryName"}, {{"todas:_las_filas", each _, type nullable table[CategoryID = nullable Int64.Type, CategoryName = nullable text]}, {"MinCategoryID", each List.Min([CategoryID]), type nullable Int64.Type}}),
  #"Filas ordenadas" = Table.Sort(#"Filas agrupadas", {{"MinCategoryID", Order.Ascending}}),
  #"Índice agregado" = Table.AddIndexColumn(#"Filas ordenadas", "NewCategoryID", 1, 1, Int64.Type)
in
  #"Índice agregado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Bronze_Silver Categories_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Silver_Layer Categories" = let
  Origen = #"Bronze_Layer Categories",
  #"Elegir columnas" = Table.SelectColumns(Origen, {"CategoryName", "NewCategoryID"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Elegir columnas", {{"NewCategoryID", "CategoryID"}}),
  #"Columnas reordenadas" = Table.ReorderColumns(#"Columnas con nombre cambiado", {"CategoryID", "CategoryName"})
in
  #"Columnas reordenadas";
shared #"Bronze_Layer Suppliers" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.Suppliers", ItemKind = "Table"]}[Data],
  #"Elegir columnas" = Table.SelectColumns(#"Navegación 2", {"SupplierID", "SupplierName", "ContactName", "Email", "Phone", "City", "Country"}),
  #"Texto recortado" = Table.TransformColumns(#"Elegir columnas", {{"SupplierName", each Text.Trim(_), type nullable text}, {"ContactName", each Text.Trim(_), type nullable text}, {"Email", each Text.Trim(_), type nullable text}, {"Phone", each Text.Trim(_), type nullable text}, {"City", each Text.Trim(_), type nullable text}, {"Country", each Text.Trim(_), type nullable text}}),
  #"Texto en minúsculas" = Table.TransformColumns(#"Texto recortado", {{"SupplierName", each Text.Lower(_), type nullable text}, {"ContactName", each Text.Lower(_), type nullable text}, {"Email", each Text.Lower(_), type nullable text}, {"Phone", each Text.Lower(_), type nullable text}, {"City", each Text.Lower(_), type nullable text}, {"Country", each Text.Lower(_), type nullable text}}),
  #"Valor reemplazado" = Table.ReplaceValue(#"Texto en minúsculas", " ", "", Replacer.ReplaceText, {"Phone"}),
  #"Valor reemplazado 1" = Table.ReplaceValue(#"Valor reemplazado", null, "No email", Replacer.ReplaceValue, {"Email"}),
  #"Filas agrupadas" = Table.Group(#"Valor reemplazado 1", {"SupplierName", "ContactName", "Email", "Phone", "City", "Country"}, {{"todas_las_filas", each _, type nullable table[SupplierID = nullable Int64.Type, SupplierName = nullable text, ContactName = nullable text, Email = nullable text, Phone = nullable text, City = nullable text, Country = nullable text]}, {"MinSupplierID", each List.Min([SupplierID]), type nullable Int64.Type}}),
  #"Filas ordenadas 1" = Table.Sort(#"Filas agrupadas", {{"MinSupplierID", Order.Ascending}}),
  #"Índice agregado" = Table.AddIndexColumn(#"Filas ordenadas 1", "NewSupplierID", 1, 1, Int64.Type)
in
  #"Índice agregado";
shared #"Mapping CustomerSegments" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.CustomerSegments", ItemKind = "Table"]}[Data],
  #"Texto recortado" = Table.TransformColumns(#"Navegación 2", {{"SegmentName", each Text.Trim(_), type nullable text}}),
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Texto recortado", {{"SegmentName", each Text.Proper(_), type nullable text}}),
  #"Filas agrupadas" = Table.Group(#"Mayúsculas aplicadas en cada palabra", {"SegmentName"}, {{"todas_las_filas", each _, type nullable table[CustomerSegmentID = nullable Int64.Type, SegmentName = nullable text]}, {"MinCustomerSegmentID", each List.Min([CustomerSegmentID]), type nullable Int64.Type}}),
  #"Filas ordenadas" = Table.Sort(#"Filas agrupadas", {{"MinCustomerSegmentID", Order.Ascending}}),
  #"Índice agregado" = Table.AddIndexColumn(#"Filas ordenadas", "NewCustomerSegmentID", 1, 1, Int64.Type),
  #"Elegir columnas" = Table.SelectColumns(#"Índice agregado", {"todas_las_filas", "NewCustomerSegmentID"}),
  #"todas_las_filas expandido" = Table.ExpandTableColumn(#"Elegir columnas", "todas_las_filas", {"CustomerSegmentID"}, {"OriginalCustomerSegmentID"})
in
  #"todas_las_filas expandido";
shared #"Mapping Customers" = let
  Origen = Lakehouse.Contents(null),
  #"Navegación 1" = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 3" = #"Navegación 2"{[Id = "Bronze_Layer.Customers", ItemKind = "Table"]}[Data],
  #"Elegir columnas 2" = Table.SelectColumns(#"Navegación 3", {"CustomerID", "CustomerSegmentID", "FirstName", "LastName", "Email", "Phone"}),
  #"Texto recortado" = Table.TransformColumns(#"Elegir columnas 2", {{"FirstName", each Text.Trim(_), type nullable text}, {"LastName", each Text.Trim(_), type nullable text}, {"Email", each Text.Trim(_), type nullable text}, {"Phone", each Text.Trim(_), type nullable text}}),

  #"Datos limpiados" =
      Table.TransformColumns(
          #"Texto recortado",
          {
              {"Email", each if _ = null or Text.Trim(_) = "" then "No email" else _, type text},
              {"Phone", each if _ = null or Text.Trim(_) = "" then "No phone number" else Text.Remove(_, {" ", "(", ")"}), type text}
          }
      ),

  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Datos limpiados", {{"FirstName", each Text.Proper(_), type nullable text}, {"LastName", each Text.Proper(_), type nullable text}}),
  #"Columna condicional insertada" = Table.TransformColumnTypes(Table.AddColumn(#"Mayúsculas aplicadas en cada palabra", "NewPhone", each if not Text.StartsWith([Phone], "+") and not Text.StartsWith([Phone], "No") then "+" & [Phone] else [Phone]), {{"NewPhone", type text}}),
  #"Consultas combinadas" = Table.NestedJoin(#"Columna condicional insertada", {"CustomerSegmentID"}, #"Mapping CustomerSegments", {"OriginalCustomerSegmentID"}, "Mapping CustomerSegments", JoinKind.LeftOuter),
  #"Mapping CustomerSegments expandido" = Table.ExpandTableColumn(#"Consultas combinadas", "Mapping CustomerSegments", {"NewCustomerSegmentID"}, {"NewCustomerSegmentID"}),
  #"Elegir columnas" = Table.SelectColumns(#"Mapping CustomerSegments expandido", {"CustomerID", "FirstName", "LastName", "Email", "NewPhone", "NewCustomerSegmentID"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Elegir columnas", {{"NewPhone", "Phone"}, {"NewCustomerSegmentID", "CustomerSegmentID"}}),
  #"Filas agrupadas" = Table.Group(#"Columnas con nombre cambiado", {"FirstName", "LastName", "Email", "Phone", "CustomerSegmentID"}, {{"todas_las_filas", each _, type nullable table[CustomerID = nullable Int64.Type, FirstName = nullable text, LastName = nullable text, Email = text, Phone = nullable text, CustomerSegmentID = nullable Int64.Type]}, {"MinCustomerID", each List.Min([CustomerID]), type nullable Int64.Type}}),
  #"Filas ordenadas" = Table.Sort(#"Filas agrupadas", {{"MinCustomerID", Order.Ascending}}),
  #"Duplicados quitados" = Table.Distinct(#"Filas ordenadas", {"FirstName", "LastName", "Email", "CustomerSegmentID"}),
  #"Índice agregado" = Table.AddIndexColumn(#"Duplicados quitados", "NewCustomerID", 1, 1, Int64.Type),
  #"Elegir columnas 1" = Table.SelectColumns(#"Índice agregado", {"FirstName", "LastName", "Email", "BirthDate", "Gender", "RegistrationDate", "Status", "Phone", "CustomerSegmentID", "NewCustomerID"}),
  #"Columnas con nombre cambiado 1" = Table.RenameColumns(#"Elegir columnas 1", {{"NewCustomerID", "CustomerID"}}),
  #"Columnas reordenadas" = Table.ReorderColumns(#"Columnas con nombre cambiado 1", {"CustomerID", "CustomerSegmentID", "FirstName", "LastName", "Email", "BirthDate", "Gender", "RegistrationDate", "Status", "Phone"})
in
  #"Columnas reordenadas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Bronze_Layer CustomerSegments_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Bronze_Layer CustomerSegments" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.CustomerSegments", ItemKind = "Table"]}[Data],
  #"Texto recortado" = Table.TransformColumns(#"Navegación 2", {{"SegmentName", each Text.Trim(_), type nullable text}}),
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Texto recortado", {{"SegmentName", each Text.Proper(_), type nullable text}}),
  #"Filas ordenadas" = Table.Sort(#"Mayúsculas aplicadas en cada palabra", {{"CustomerSegmentID", Order.Ascending}}),
  #"Duplicados quitados" = Table.Distinct(#"Filas ordenadas", {"SegmentName"}),
  #"Índice agregado" = Table.AddIndexColumn(#"Duplicados quitados", "NewCustomerSegmentID", 1, 1, Int64.Type),
  #"Elegir columnas" = Table.SelectColumns(#"Índice agregado", {"SegmentName", "NewCustomerSegmentID"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Elegir columnas", {{"NewCustomerSegmentID", "CustomerSegmentID"}}),
  #"Columnas reordenadas" = Table.ReorderColumns(#"Columnas con nombre cambiado", {"CustomerSegmentID", "SegmentName"})
in
  #"Columnas reordenadas";
shared #"Bronze_Layer Customers" = let
  Origen = Lakehouse.Contents(null),
  #"Navegación 1" = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 3" = #"Navegación 2"{[Id = "Bronze_Layer.Customers", ItemKind = "Table"]}[Data],
  #"Texto recortado" = Table.TransformColumns(#"Navegación 3", {{"FirstName", each Text.Trim(_), type nullable text}, {"LastName", each Text.Trim(_), type nullable text}, {"Email", each Text.Trim(_), type nullable text}, {"Phone", each Text.Trim(_), type nullable text}, {"Gender", each Text.Trim(_), type nullable text}, {"Status", each Text.Trim(_), type nullable text}}),
  #"Texto en minúsculas" = Table.TransformColumns(#"Texto recortado", {{"Status", each Text.Lower(_), type nullable text}}),

  #"Datos limpiados" =
      Table.TransformColumns(
          #"Texto en minúsculas",
          {
              {"Email", each if _ = null or Text.Trim(_) = "" then "No email" else _, type text},
              {"Phone", each if _ = null or Text.Trim(_) = "" then "No phone number" else Text.Remove(_, {" ", "(", ")"}), type text}
          }
      ),


  #"Gender estandarizado" =
      Table.TransformColumns(
          #"Datos limpiados",
          {
              {"Gender", each
                  if _ = null or Text.Trim(_) = "" or List.Contains({"M", "N/A", "Unknown"}, Text.Trim(_)) then "No gender"
                  else if List.Contains({"Femenino", "Mujer", "F"}, Text.Trim(_)) then "Female"
                  else if List.Contains({"Masculino", "Hombre", "H"}, Text.Trim(_)) then "Male"
                  else _, type text}
          }
      ),

    #"Status estandarizado" =
      Table.TransformColumns(
          #"Gender estandarizado",
          {
              {"Status", each
                  if _ = null or Text.Trim(_) = "" or List.Contains({"i", "inactive", "inactivo", "n/a"}, Text.Trim(_)) then "Inactive"
                  else if List.Contains({"a", "active", "activo"}, Text.Trim(_)) then "Active"
                  else _, type text}
          }
      ),

  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Status estandarizado", {{"FirstName", each Text.Proper(_), type nullable text}, {"LastName", each Text.Proper(_), type nullable text}}),
  #"Columna condicional insertada" = Table.TransformColumnTypes(Table.AddColumn(#"Mayúsculas aplicadas en cada palabra", "NewPhone", each if not Text.StartsWith([Phone], "+") and not Text.StartsWith([Phone], "No") then "+" & [Phone] else [Phone]), {{"NewPhone", type text}}),
  #"Consultas combinadas" = Table.NestedJoin(#"Columna condicional insertada", {"CustomerSegmentID"}, #"Mapping CustomerSegments", {"OriginalCustomerSegmentID"}, "Mapping CustomerSegments", JoinKind.LeftOuter),
  #"Mapping CustomerSegments expandido" = Table.ExpandTableColumn(#"Consultas combinadas", "Mapping CustomerSegments", {"NewCustomerSegmentID"}, {"NewCustomerSegmentID"}),
  #"Elegir columnas" = Table.SelectColumns(#"Mapping CustomerSegments expandido", {"CustomerID", "FirstName", "LastName", "Email", "BirthDate", "Gender", "RegistrationDate", "Status", "NewPhone", "NewCustomerSegmentID"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Elegir columnas", {{"NewPhone", "Phone"}, {"NewCustomerSegmentID", "CustomerSegmentID"}}),
  #"Filas ordenadas" = Table.Sort(#"Columnas con nombre cambiado", {{"CustomerID", Order.Ascending}}),
  #"Duplicados quitados" = Table.Distinct(#"Filas ordenadas", {"FirstName", "LastName", "Email", "CustomerSegmentID"}),
  #"Índice agregado" = Table.AddIndexColumn(#"Duplicados quitados", "NewCustomerID", 1, 1, Int64.Type),
  #"Elegir columnas 1" = Table.SelectColumns(#"Índice agregado", {"FirstName", "LastName", "Email", "BirthDate", "Gender", "RegistrationDate", "Status", "Phone", "CustomerSegmentID", "NewCustomerID"}),
  #"Columnas con nombre cambiado 1" = Table.RenameColumns(#"Elegir columnas 1", {{"NewCustomerID", "CustomerID"}}),
  #"Columnas reordenadas" = Table.ReorderColumns(#"Columnas con nombre cambiado 1", {"CustomerID", "CustomerSegmentID", "FirstName", "LastName", "Email", "BirthDate", "Gender", "RegistrationDate", "Status", "Phone"})
in
  #"Columnas reordenadas";
shared #"Bronze_Layer CustomerAddresses" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.CustomerAddresses", ItemKind = "Table"]}[Data]
in
  #"Navegación 2";
shared #"Bronze_Layer Employees" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.Employees", ItemKind = "Table"]}[Data]
in
  #"Navegación 2";
shared #"Bronze_Layer Stores" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.Stores", ItemKind = "Table"]}[Data]
in
  #"Navegación 2";
shared #"Bronze_Layer CustomerSegments_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Staging_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "CustomerSegments_Silver_Staging", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Mapping Categories" = let
  Origen = #"Bronze_Layer Categories",
  #"todas:_las_filas expandido" = Table.ExpandTableColumn(Origen, "todas:_las_filas", {"CategoryID"}, {"OriginalCategoryID"}),
  #"Elegir columnas" = Table.SelectColumns(#"todas:_las_filas expandido", {"OriginalCategoryID", "NewCategoryID"})
in
  #"Elegir columnas";
shared #"Bronze_Silver Categories_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Silver_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Categories_Silver", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Silver_Layer Suppliers_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Silver_Layer Suppliers" = let
  Origen = #"Bronze_Layer Suppliers",
  #"Elegir columnas 1" = Table.SelectColumns(Origen, {"SupplierName", "ContactName", "Email", "Phone", "City", "Country", "NewSupplierID"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Elegir columnas 1", {{"NewSupplierID", "SupplierID"}}),
  #"Texto en mayúsculas" = Table.TransformColumns(#"Columnas con nombre cambiado", {{"SupplierName", each Text.Upper(_), type nullable text}}),
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Texto en mayúsculas", {{"ContactName", each Text.Proper(_), type nullable text}, {"City", each Text.Proper(_), type nullable text}, {"Country", each Text.Proper(_), type nullable text}}),
  #"Columnas reordenadas" = Table.ReorderColumns(#"Mayúsculas aplicadas en cada palabra", {"SupplierID", "SupplierName", "ContactName", "Email", "Phone", "City", "Country"})
in
  #"Columnas reordenadas";
shared #"Silver_Layer Suppliers_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Silver_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Suppliers_Silver", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Mappin Suppliers" = let
  Origen = #"Bronze_Layer Suppliers",
  #"todas_las_filas expandido" = Table.ExpandTableColumn(Origen, "todas_las_filas", {"SupplierID"}, {"OriginalSupplierID"}),
  #"Elegir columnas" = Table.SelectColumns(#"todas_las_filas expandido", {"OriginalSupplierID", "NewSupplierID"})
in
  #"Elegir columnas";
