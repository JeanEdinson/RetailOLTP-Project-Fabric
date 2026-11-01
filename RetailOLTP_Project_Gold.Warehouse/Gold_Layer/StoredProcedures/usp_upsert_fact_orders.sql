CREATE   PROCEDURE [Gold_Layer].[usp_upsert_fact_orders]
AS
BEGIN
    SET NOCOUNT ON;

    EXEC [Gold_Layer].[usp_upsert_dos_claves] @Tabla = 'fact_orders', @Clave = 'OrderID', @Clave2 = 'OrderDetailID';
END;

GO