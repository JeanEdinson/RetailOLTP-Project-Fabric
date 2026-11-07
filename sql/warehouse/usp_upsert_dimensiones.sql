CREATE OR ALTER PROCEDURE [Gold_Layer].[usp_upsert_dimensiones]
AS
BEGIN
    SET NOCOUNT ON;

    EXEC [Gold_Layer].[usp_upsert_una_clave] @Tabla = 'dim_customers', @Clave = 'CustomerID';
    EXEC [Gold_Layer].[usp_upsert_una_clave] @Tabla = 'dim_employees', @Clave = 'EmployeeID';
    EXEC [Gold_Layer].[usp_upsert_una_clave] @Tabla = 'dim_products',  @Clave = 'ProductID';
    EXEC [Gold_Layer].[usp_upsert_una_clave] @Tabla = 'dim_stores',    @Clave = 'StoreID';
END;
