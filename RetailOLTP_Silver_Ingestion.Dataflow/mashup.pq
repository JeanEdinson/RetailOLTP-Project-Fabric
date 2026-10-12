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
shared #"Bronze_Layer Products" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.Products", ItemKind = "Table"]}[Data],
  #"Texto recortado" = Table.TransformColumns(#"Navegación 2", {{"SKU", each Text.Trim(_), type nullable text}, {"ProductName", each Text.Trim(_), type nullable text}, {"Status", each Text.Trim(_), type nullable text}}),
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Texto recortado", {{"ProductName", each Text.Proper(_), type nullable text}, {"Status", each Text.Proper(_), type nullable text}}),
  #"Personalizado agregado" = Table.TransformColumnTypes(Table.AddColumn(#"Mayúsculas aplicadas en cada palabra", "NewUnitPrice", each if [UnitPrice] = null then 1.6 * [CostPrice] else [UnitPrice]), {{"NewUnitPrice", type number}}),
  #"Status estandarizado" =
    Table.TransformColumns(
      #"Personalizado agregado",
      {
        {"Status", each
          if _ = null or Text.Trim(_) = "" or List.Contains({"I", "Inactive", "Inactivo", "N/A"}, Text.Trim(_)) then "Inactive"
          else if List.Contains({"A", "Active", "Activo"}, Text.Trim(_)) then "Active"
          else _, type text}
        }
    ),
  #"Consultas combinadas" = Table.NestedJoin(#"Status estandarizado", {"CategoryID"}, #"Mapping Categories", {"OriginalCategoryID"}, "Mapping Categories", JoinKind.LeftOuter),
  #"Mapping Categories expandido" = Table.ExpandTableColumn(#"Consultas combinadas", "Mapping Categories", {"NewCategoryID"}, {"NewCategoryID"}),
  #"Consultas combinadas 1" = Table.NestedJoin(#"Mapping Categories expandido", {"SupplierID"}, #"Mapping Suppliers", {"OriginalSupplierID"}, "Mapping Suppliers", JoinKind.LeftOuter),
  #"Mapping Suppliers expandido" = Table.ExpandTableColumn(#"Consultas combinadas 1", "Mapping Suppliers", {"NewSupplierID"}, {"NewSupplierID"}),
  #"Elegir columnas" = Table.SelectColumns(#"Mapping Suppliers expandido", {"ProductID", "SKU", "ProductName", "CostPrice", "UnitsPerPackage", "Status", "NewUnitPrice", "NewCategoryID", "NewSupplierID"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Elegir columnas", {{"NewUnitPrice", "UnitPrice"}, {"NewCategoryID", "CategoryID"}, {"NewSupplierID", "SupplierID"}}),
  #"Filas agrupadas" = Table.Group(#"Columnas con nombre cambiado", {"SKU", "ProductName", "CategoryID", "SupplierID"}, {{"todas_las_filas", each _, type nullable table[ProductID = nullable Int64.Type, SKU = nullable text, ProductName = nullable text, CostPrice = nullable number, UnitsPerPackage = nullable Int64.Type, Status = text, UnitPrice = nullable number, CategoryID = nullable Int64.Type, SupplierID = nullable Int64.Type]}, {"MinProductID", each List.Min([ProductID]), type nullable Int64.Type}}),
  #"Filas ordenadas" = Table.Sort(#"Filas agrupadas", {{"MinProductID", Order.Ascending}}),
  #"Índice agregado" = Table.AddIndexColumn(#"Filas ordenadas", "NewProductID", 1, 1, Int64.Type)
in
  #"Índice agregado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Silver_Layer Products_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Silver_Layer Products" = let
  Origen = #"Bronze_Layer Products",
  #"todas_las_filas expandido" = Table.ExpandTableColumn(Origen, "todas_las_filas", {"CostPrice", "UnitsPerPackage", "Status", "UnitPrice"}, {"CostPrice", "UnitsPerPackage", "Status", "UnitPrice"}),
  #"Duplicados quitados" = Table.Distinct(#"todas_las_filas expandido", {"NewProductID"}),
  #"Elegir columnas" = Table.SelectColumns(#"Duplicados quitados", {"SKU", "ProductName", "CategoryID", "SupplierID", "CostPrice", "UnitsPerPackage", "Status", "UnitPrice", "NewProductID"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Elegir columnas", {{"NewProductID", "ProductID"}}),
  #"Columnas reordenadas" = Table.ReorderColumns(#"Columnas con nombre cambiado", {"ProductID", "SKU", "ProductName", "CategoryID", "SupplierID", "CostPrice", "UnitsPerPackage", "Status", "UnitPrice"})
in
  #"Columnas reordenadas";
shared #"Bronze_Layer CustomerSegments" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.CustomerSegments", ItemKind = "Table"]}[Data],
  #"Texto recortado" = Table.TransformColumns(#"Navegación 2", {{"SegmentName", each Text.Trim(_), type nullable text}}),
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Texto recortado", {{"SegmentName", each Text.Proper(_), type nullable text}}),
  #"Filas agrupadas" = Table.Group(#"Mayúsculas aplicadas en cada palabra", {"SegmentName"}, {{"todas_las_filas", each _, type nullable table[CustomerSegmentID = nullable Int64.Type, SegmentName = nullable text]}, {"MinCustomerSegmentID", each List.Min([CustomerSegmentID]), type nullable Int64.Type}}),
  #"Filas ordenadas" = Table.Sort(#"Filas agrupadas", {{"MinCustomerSegmentID", Order.Ascending}}),
  #"Índice agregado" = Table.AddIndexColumn(#"Filas ordenadas", "NewCustomerSegmentID", 1, 1, Int64.Type)
in
  #"Índice agregado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Silver_Layer CustomerSegments_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Silver_Layer CustomerSegments" = let
  Origen = #"Bronze_Layer CustomerSegments",
  #"Elegir columnas" = Table.SelectColumns(Origen, {"SegmentName", "NewCustomerSegmentID"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Elegir columnas", {{"NewCustomerSegmentID", "CustomerSegmentID"}}),
  #"Columnas reordenadas" = Table.ReorderColumns(#"Columnas con nombre cambiado", {"CustomerSegmentID", "SegmentName"})
in
  #"Columnas reordenadas";
shared #"Mapping CustomerSegments" = let
  Origen = #"Bronze_Layer CustomerSegments",
  #"todas_las_filas expandido" = Table.ExpandTableColumn(Origen, "todas_las_filas", {"CustomerSegmentID"}, {"OriginalCustomerSegmentID"}),
  #"Elegir columnas" = Table.SelectColumns(#"todas_las_filas expandido", {"OriginalCustomerSegmentID", "NewCustomerSegmentID"})
in
  #"Elegir columnas";
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
  #"Filas agrupadas" = Table.Group(#"Columnas con nombre cambiado", {"FirstName", "LastName", "Email", "CustomerSegmentID"}, {{"todas_las_filas", each _, type nullable table[CustomerID = nullable Int64.Type, FirstName = nullable text, LastName = nullable text, Email = text, BirthDate = nullable date, Gender = text, RegistrationDate = nullable date, Status = text, Phone = nullable text, CustomerSegmentID = nullable Int64.Type]}, {"MinCustomerID", each List.Min([CustomerID]), type nullable Int64.Type}}),
  #"Filas ordenadas" = Table.Sort(#"Filas agrupadas", {{"MinCustomerID", Order.Ascending}}),
  #"Índice agregado" = Table.AddIndexColumn(#"Filas ordenadas", "NewCustomerID", 1, 1, Int64.Type)
in
  #"Índice agregado";
shared #"Bronze_Layer CustomerAddresses" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.CustomerAddresses", ItemKind = "Table"]}[Data],
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Navegación 2", {{"AddressType", each Text.Proper(_), type nullable text}, {"AddressLine1", each Text.Proper(_), type nullable text}, {"City", each Text.Proper(_), type nullable text}, {"State", each Text.Proper(_), type nullable text}, {"Country", each Text.Proper(_), type nullable text}}),
  #"Texto recortado" = Table.TransformColumns(#"Mayúsculas aplicadas en cada palabra", {{"AddressType", each Text.Trim(_), type nullable text}, {"AddressLine1", each Text.Trim(_), type nullable text}, {"City", each Text.Trim(_), type nullable text}, {"State", each Text.Trim(_), type nullable text}, {"Country", each Text.Trim(_), type nullable text}}),
  #"Address estandarizado" =
    Table.TransformColumns(
      #"Texto recortado",
        {
          {"AddressType", each
            if _ = null or _ = "" then "No address type"
            else if List.Contains({"Domicilio", "Casa"}, Text.Trim(_)) then "Home"
            else if List.Contains({"Trabajo"}, Text.Trim(_)) then "Work"
            else _, type text}
        }
    ),
  #"Country estandarizado" =
    Table.TransformColumns(
      #"Address estandarizado",
        {
          {"Country", each
            if List.Contains({"Pe", "Per", "Perú"}, Text.Trim(_)) then "Peru"
            else _, type text}
        }
    ),
  #"Consultas combinadas" = Table.NestedJoin(#"Country estandarizado", {"CustomerID"}, #"Mapping Customers", {"OriginalCustomerID"}, "Mapping Customers", JoinKind.LeftOuter),
  #"Mapping Customers expandido" = Table.ExpandTableColumn(#"Consultas combinadas", "Mapping Customers", {"NewCustomerID"}, {"NewCustomerID"}),
  #"Filas agrupadas" = Table.Group(#"Mapping Customers expandido", {"AddressType", "AddressLine1", "City", "State", "PostalCode", "NewCustomerID"}, {{"todas_las_filas", each _, type nullable table[CustomerAddressID = nullable Int64.Type, CustomerID = nullable Int64.Type, AddressType = text, AddressLine1 = nullable text, AddressLine2 = nullable text, City = nullable text, State = nullable text, PostalCode = nullable text, Country = text, IsPrimary = nullable logical, NewCustomerID = nullable Int64.Type]}, {"MinCustomerAddressID", each List.Min([CustomerAddressID]), type nullable Int64.Type}}),
  #"Filas ordenadas" = Table.Sort(#"Filas agrupadas", {{"MinCustomerAddressID", Order.Ascending}}),
  #"Índice agregado" = Table.AddIndexColumn(#"Filas ordenadas", "NewCustomerAddressesID", 1, 1, Int64.Type)
in
  #"Índice agregado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Bronze_Silver CustomerAddresses_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Bronze_Silver CustomerAddresses" = let
  Origen = #"Bronze_Layer CustomerAddresses",
  #"todas_las_filas expandido" = Table.ExpandTableColumn(Origen, "todas_las_filas", {"AddressLine2", "Country", "IsPrimary"}, {"AddressLine2", "Country", "IsPrimary"}),
  #"Elegir columnas" = Table.SelectColumns(#"todas_las_filas expandido", {"AddressType", "AddressLine1", "City", "State", "PostalCode", "NewCustomerID", "AddressLine2", "Country", "IsPrimary", "NewCustomerAddressesID"}),
  #"Duplicados quitados" = Table.Distinct(#"Elegir columnas", {"NewCustomerAddressesID"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Duplicados quitados", {{"NewCustomerAddressesID", "CustomerAddressesID"}, {"NewCustomerID", "CustomerID"}}),
  #"Columnas reordenadas" = Table.ReorderColumns(#"Columnas con nombre cambiado", {"CustomerAddressesID", "CustomerID", "AddressType", "AddressLine1", "AddressLine2", "City", "State", "PostalCode", "Country", "IsPrimary"})
in
  #"Columnas reordenadas";
shared #"Mapping CustomerAddresses" = let
  Origen = #"Bronze_Layer CustomerAddresses",
  #"todas_las_filas expandido" = Table.ExpandTableColumn(Origen, "todas_las_filas", {"CustomerAddressID"}, {"OriginalCustomerAddressID"}),
  #"Elegir columnas" = Table.SelectColumns(#"todas_las_filas expandido", {"OriginalCustomerAddressID", "NewCustomerAddressesID"})
in
  #"Elegir columnas";
shared #"Bronze_Layer Stores" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.Stores", ItemKind = "Table"]}[Data],
  #"Texto recortado" = Table.TransformColumns(#"Navegación 2", {{"StoreCode", each Text.Trim(_), type nullable text}, {"StoreName", each Text.Trim(_), type nullable text}, {"Address", each Text.Trim(_), type nullable text}, {"City", each Text.Trim(_), type nullable text}, {"State", each Text.Trim(_), type nullable text}, {"Country", each Text.Trim(_), type nullable text}, {"StoreType", each Text.Trim(_), type nullable text}, {"Status", each Text.Trim(_), type nullable text}}),
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Texto recortado", {{"StoreName", each Text.Proper(_), type nullable text}, {"Address", each Text.Proper(_), type nullable text}, {"City", each Text.Proper(_), type nullable text}, {"State", each Text.Proper(_), type nullable text}, {"Country", each Text.Proper(_), type nullable text}, {"StoreType", each Text.Proper(_), type nullable text}, {"Status", each Text.Proper(_), type nullable text}}),
  #"Status estandarizado" =
    Table.TransformColumns(
        #"Mayúsculas aplicadas en cada palabra",
        {
            {"Status", each
                if _ = null or Text.Trim(_) = "" or List.Contains({"I", "Inactive", "Inactivo", "N/A"}, Text.Trim(_)) then "Inactive"
                else if List.Contains({"A", "Active", "Activo"}, Text.Trim(_)) then "Active"
                else _, type text}
        }
    ),
  #"Filas agrupadas" = Table.Group(#"Status estandarizado", {"StoreCode", "StoreName", "Address", "City", "State", "StoreType", "Status"}, {{"toddas_las_filas", each _, type nullable table[StoreID = nullable Int64.Type, StoreCode = nullable text, StoreName = nullable text, Address = nullable text, City = nullable text, State = nullable text, Country = nullable text, OpeningDate = nullable date, StoreType = nullable text, Status = text]}, {"MinStoreID", each List.Min([StoreID]), type nullable Int64.Type}}),
  #"Filas ordenadas" = Table.Sort(#"Filas agrupadas", {{"MinStoreID", Order.Ascending}}),
  #"Índice agregado" = Table.AddIndexColumn(#"Filas ordenadas", "NewStoreID", 1, 1, Int64.Type)
in
  #"Índice agregado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Silver_Layer Stores_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Silver_Layer Stores" = let
  Origen = #"Bronze_Layer Stores",
  #"toddas_las_filas expandido" = Table.ExpandTableColumn(Origen, "toddas_las_filas", {"Country", "OpeningDate"}, {"Country", "OpeningDate"}),
  #"Duplicados quitados" = Table.Distinct(#"toddas_las_filas expandido", {"NewStoreID"}),
  #"Elegir columnas" = Table.SelectColumns(#"Duplicados quitados", {"StoreCode", "StoreName", "Address", "City", "State", "StoreType", "Status", "Country", "OpeningDate", "NewStoreID"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Elegir columnas", {{"NewStoreID", "StoreID"}}),
  #"Columnas reordenadas" = Table.ReorderColumns(#"Columnas con nombre cambiado", {"StoreID", "StoreCode", "StoreName", "Address", "City", "State", "Country", "StoreType", "OpeningDate", "Status"})
in
  #"Columnas reordenadas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Mapping Stores_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Mapping Stores" = let
  Origen = #"Bronze_Layer Stores",
  #"toddas_las_filas expandido" = Table.ExpandTableColumn(Origen, "toddas_las_filas", {"StoreID"}, {"OriginalStoreID"}),
  #"Elegir columnas" = Table.SelectColumns(#"toddas_las_filas expandido", {"OriginalStoreID", "NewStoreID"})
in
  #"Elegir columnas";
shared #"Bronze_Layer Employees" = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "Bronze_Layer.Employees", ItemKind = "Table"]}[Data],
  #"Texto recortado" = Table.TransformColumns(#"Navegación 2", {{"FirstName", each Text.Trim(_), type nullable text}, {"LastName", each Text.Trim(_), type nullable text}, {"Email", each Text.Trim(_), type nullable text}, {"JobTitle", each Text.Trim(_), type nullable text}, {"Status", each Text.Trim(_), type nullable text}}),
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Texto recortado", {{"FirstName", each Text.Proper(_), type nullable text}, {"LastName", each Text.Proper(_), type nullable text}, {"JobTitle", each Text.Proper(_), type nullable text}, {"Status", each Text.Proper(_), type nullable text}}),
  #"Status estandarizado" =
    Table.TransformColumns(
        #"Mayúsculas aplicadas en cada palabra",
        {
            {"Status", each
                if _ = null or Text.Trim(_) = "" or List.Contains({"I", "Inactive", "Inactivo", "N/A", "Unknown"}, Text.Trim(_)) then "Inactive"
                else if List.Contains({"A", "Active", "Activo"}, Text.Trim(_)) then "Active"
                else _, type text}
        }
    ),
  #"Consultas combinadas" = Table.NestedJoin(#"Status estandarizado", {"StoreID"}, #"Mapping Stores", {"OriginalStoreID"}, "Mapping Stores", JoinKind.LeftOuter),
  #"Mapping Stores expandido" = Table.ExpandTableColumn(#"Consultas combinadas", "Mapping Stores", {"NewStoreID"}, {"NewStoreID"}),
  #"Filas agrupadas" = Table.Group(#"Mapping Stores expandido", {"FirstName", "LastName", "Email", "Status", "NewStoreID"}, {{"todas_las_filas", each _, type nullable table[EmployeeID = nullable Int64.Type, StoreID = nullable Int64.Type, FirstName = nullable text, LastName = nullable text, Email = nullable text, JobTitle = nullable text, HireDate = nullable date, Status = text, NewStoreID = nullable Int64.Type]}, {"MinEmployeeID", each List.Min([EmployeeID]), type nullable Int64.Type}}),
  #"Filas ordenadas" = Table.Sort(#"Filas agrupadas", {{"MinEmployeeID", Order.Ascending}}),
  #"Índice agregado" = Table.AddIndexColumn(#"Filas ordenadas", "NewEmployeeID", 1, 1, Int64.Type)
in
  #"Índice agregado";
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
shared #"Mapping Suppliers" = let
  Origen = #"Bronze_Layer Suppliers",
  #"todas_las_filas expandido" = Table.ExpandTableColumn(Origen, "todas_las_filas", {"SupplierID"}, {"OriginalSupplierID"}),
  #"Elegir columnas" = Table.SelectColumns(#"todas_las_filas expandido", {"OriginalSupplierID", "NewSupplierID"})
in
  #"Elegir columnas";
shared #"Silver_Layer Products_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Silver_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Products_Silver", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Mapping Products_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Mapping Products" = let
  Origen = #"Bronze_Layer Products",
  #"Elegir columnas" = Table.SelectColumns(Origen, {"todas_las_filas", "NewProductID"}),
  #"todas_las_filas expandido" = Table.ExpandTableColumn(#"Elegir columnas", "todas_las_filas", {"ProductID"}, {"OriginalProductID"})
in
  #"todas_las_filas expandido";
shared #"Mapping Products_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Mapping_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Products_Mapping", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Silver_Layer CustomerSegments_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Silver_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "CustomerSegments_Silver", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Silver_Layer Customers_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Silver_Layer Customers" = let
  Origen = #"Bronze_Layer Customers",
  #"todas_las_filas expandido" = Table.ExpandTableColumn(Origen, "todas_las_filas", {"BirthDate", "Gender", "RegistrationDate", "Status", "Phone"}, {"BirthDate", "Gender", "RegistrationDate", "Status", "Phone"}),
  #"Elegir columnas" = Table.SelectColumns(#"todas_las_filas expandido", {"FirstName", "LastName", "Email", "CustomerSegmentID", "BirthDate", "Gender", "RegistrationDate", "Status", "Phone", "NewCustomerID"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Elegir columnas", {{"NewCustomerID", "CustomerID"}})
in
  #"Columnas con nombre cambiado";
shared #"Silver_Layer Customers_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Silver_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Customers_Silver", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Mapping Customers_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Mapping Customers" = let
  Origen = #"Bronze_Layer Customers",
  #"todas_las_filas expandido" = Table.ExpandTableColumn(Origen, "todas_las_filas", {"CustomerID"}, {"OriginalCustomerID"}),
  #"Elegir columnas" = Table.SelectColumns(#"todas_las_filas expandido", {"OriginalCustomerID", "NewCustomerID"})
in
  #"Elegir columnas";
shared #"Mapping Customers_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Mapping_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Customers_Mapping", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Bronze_Silver CustomerAddresses_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Silver_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "CustomerAddresses_Silver", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Silver_Layer Stores_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Silver_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Stores_Silver", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Mapping Stores_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Mapping_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Stores_Mapping", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Silver_Layer Employees_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Silver_Layer Employees" = let
  Origen = #"Bronze_Layer Employees",
  #"todas_las_filas expandido" = Table.ExpandTableColumn(Origen, "todas_las_filas", {"JobTitle", "HireDate"}, {"JobTitle", "HireDate"}),
  #"Duplicados quitados" = Table.Distinct(#"todas_las_filas expandido", {"NewEmployeeID"}),
  #"Elegir columnas" = Table.SelectColumns(#"Duplicados quitados", {"FirstName", "LastName", "Email", "Status", "NewStoreID", "JobTitle", "HireDate", "NewEmployeeID"}),
  #"Columnas reordenadas" = Table.ReorderColumns(#"Elegir columnas", {"NewEmployeeID", "FirstName", "LastName", "Email", "Status", "NewStoreID", "JobTitle", "HireDate"}),
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Columnas reordenadas", {{"NewEmployeeID", "EmployeeID"}})
in
  #"Columnas con nombre cambiado";
shared #"Silver_Layer Employees_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Silver_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Employees_Silver", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Mapping Employees_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Mapping Employees" = let
  Origen = #"Bronze_Layer Employees",
  #"todas_las_filas expandido" = Table.ExpandTableColumn(Origen, "todas_las_filas", {"EmployeeID"}, {"EmployeeID"}),
  #"Elegir columnas" = Table.SelectColumns(#"todas_las_filas expandido", {"EmployeeID", "NewEmployeeID"})
in
  #"Elegir columnas";
shared #"Mapping Employees_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "033013a2-b416-4b09-96e7-2f911dbac52c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "15094689-301b-4cc4-b32a-9fe54b60893a"]}[Data],
  Navigation_3 = Navigation_2{[Id = "Mapping_Layer", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "Employees_Mapping", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
