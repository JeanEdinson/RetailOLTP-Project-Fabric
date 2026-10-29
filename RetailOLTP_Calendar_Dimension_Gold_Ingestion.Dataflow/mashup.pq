[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Dim_Calendar_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Dim_Calendar = let
    FechaInicio = #date(2020, 1, 1),
    FechaFin    = Date.EndOfYear(Date.From(DateTime.LocalNow())),   // 31/12 del año actual
    Cultura     = "en-US",

    Fechas = List.Dates(
        FechaInicio,
        Duration.Days(FechaFin - FechaInicio) + 1,
        #duration(1, 0, 0, 0)
    ),
    Origen = Table.FromList(
        Fechas,
        Splitter.SplitByNothing(),
        type table [date = date]
    ),

    DayNum = Table.AddColumn(Origen, "day_num",
        each Date.DayOfWeek([date], Day.Monday) + 1, Int64.Type),

    DayName = Table.AddColumn(DayNum, "day_name",
        each Date.DayOfWeekName([date], Cultura), type text),

    Week = Table.AddColumn(DayName, "week",
        each Date.WeekOfYear([date], Day.Monday), Int64.Type),

    // ISO 8601: la semana pertenece al año de su jueves
    IsoWeek = Table.AddColumn(Week, "iso_week",
        each
            let
                Jueves = Date.AddDays([date], 3 - Date.DayOfWeek([date], Day.Monday))
            in
                Number.IntegerDivide(Date.DayOfYear(Jueves) - 1, 7) + 1,
        Int64.Type),

    IsoYear = Table.AddColumn(IsoWeek, "iso_year",
        each Date.Year(Date.AddDays([date], 3 - Date.DayOfWeek([date], Day.Monday))),
        Int64.Type),

    Month = Table.AddColumn(IsoYear, "month",
        each Date.Month([date]), Int64.Type),

    MonthName = Table.AddColumn(Month, "month_name",
        each Date.MonthName([date], Cultura), type text),

    Quarter = Table.AddColumn(MonthName, "quarter",
        each "Q" & Text.From(Date.QuarterOfYear([date])), type text),

    Year = Table.AddColumn(Quarter, "year",
        each Date.Year([date]), Int64.Type),

    // ===== Clave primaria =====
    Resultado = Table.AddKey(Year, {"date"}, true)
in
    Resultado;
shared Dim_Calendar_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = true, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "RetailOLTP_Project_Gold"]}[Data],
  Navigation_3 = Navigation_2{[Schema = "Gold_Layer"]}[Data],
  TableNavigation = Navigation_3{[Name = "dim_calendar"]}?[Data]?
in
  TableNavigation;
