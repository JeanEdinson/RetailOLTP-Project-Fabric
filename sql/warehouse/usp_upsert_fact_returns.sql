CREATE OR ALTER PROCEDURE [Gold_Layer].[usp_upsert_fact_returns]
AS
BEGIN
    SET NOCOUNT ON;

    EXEC [Gold_Layer].[usp_upsert_una_clave] @Tabla = 'fact_returns', @Clave = 'ReturnID';
END;
