CREATE   PROCEDURE [Gold_Layer].[usp_upsert_dimension]
    @Tabla          VARCHAR(128),
    @Clave          VARCHAR(128),
    @EsquemaStaging VARCHAR(128) = 'Staging_Layer',
    @EsquemaGold    VARCHAR(128) = 'Gold_Layer',
    @SufijoStaging  VARCHAR(20)  = '_staging'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @TablaStaging VARCHAR(128) = @Tabla + @SufijoStaging;
    DECLARE @Origen  NVARCHAR(400) = QUOTENAME(@EsquemaStaging) + N'.' + QUOTENAME(@TablaStaging);
    DECLARE @Destino NVARCHAR(400) = QUOTENAME(@EsquemaGold)    + N'.' + QUOTENAME(@Tabla);
    DECLARE @Msg NVARCHAR(2000);

    -- 1. Las tablas existen y tienen las mismas columnas
    DECLARE @NColsGold INT, @NColsStg INT, @Diferencias INT;

    SELECT @NColsGold = COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @EsquemaGold AND TABLE_NAME = @Tabla;

    SELECT @NColsStg = COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @EsquemaStaging AND TABLE_NAME = @TablaStaging;

    IF @NColsGold = 0 OR @NColsStg = 0
    BEGIN
        SET @Msg = N'No se encontraron columnas para ' + @Destino + N' o ' + @Origen + N'.';
        THROW 50001, @Msg, 1;
    END;

    SELECT @Diferencias = COUNT(*)
    FROM (SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = @EsquemaGold AND TABLE_NAME = @Tabla) AS G
    FULL OUTER JOIN
         (SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = @EsquemaStaging AND TABLE_NAME = @TablaStaging) AS S
        ON G.COLUMN_NAME = S.COLUMN_NAME
    WHERE G.COLUMN_NAME IS NULL OR S.COLUMN_NAME IS NULL;

    IF @Diferencias > 0
    BEGIN
        SET @Msg = N'Las columnas no coinciden entre ' + @Origen + N' y ' + @Destino + N'.';
        THROW 50002, @Msg, 1;
    END;

    -- 2. Clave y columnas de auditoría
    DECLARE @ClaveReal VARCHAR(128), @ColUpdatedAt VARCHAR(128),
            @ColBatchId VARCHAR(128), @TieneCreatedAt INT;

    SELECT
        @ClaveReal      = MAX(CASE WHEN LOWER(COLUMN_NAME) = LOWER(@Clave) THEN COLUMN_NAME END),
        @ColUpdatedAt   = MAX(CASE WHEN LOWER(COLUMN_NAME) = 'updatedat' THEN COLUMN_NAME END),
        @ColBatchId     = MAX(CASE WHEN LOWER(COLUMN_NAME) = 'batchid'   THEN COLUMN_NAME END),
        @TieneCreatedAt = MAX(CASE WHEN LOWER(COLUMN_NAME) = 'createdat' THEN 1 ELSE 0 END)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @EsquemaGold AND TABLE_NAME = @Tabla;

    IF @ClaveReal IS NULL
    BEGIN
        SET @Msg = N'No existe la clave ' + @Clave + N' en ' + @Destino + N'.';
        THROW 50003, @Msg, 1;
    END;

    IF @ColUpdatedAt IS NULL OR @ColBatchId IS NULL OR @TieneCreatedAt = 0
    BEGIN
        SET @Msg = N'Faltan columnas de auditoría (CreatedAt, UpdatedAt, BatchId) en ' + @Destino + N'.';
        THROW 50004, @Msg, 1;
    END;

    -- 3. Validar claves nulas o duplicadas en staging
    DECLARE @Nulos BIGINT, @Duplicados BIGINT;
    DECLARE @SqlValidar NVARCHAR(MAX) =
        N'SELECT @n = SUM(CASE WHEN ' + QUOTENAME(@ClaveReal) + N' IS NULL THEN 1 ELSE 0 END), '
      + N'@d = COUNT(' + QUOTENAME(@ClaveReal) + N') - COUNT(DISTINCT ' + QUOTENAME(@ClaveReal) + N') '
      + N'FROM ' + @Origen + N';';

    EXEC sp_executesql @SqlValidar,
         N'@n BIGINT OUTPUT, @d BIGINT OUTPUT',
         @n = @Nulos OUTPUT, @d = @Duplicados OUTPUT;

    IF ISNULL(@Nulos, 0) > 0 OR ISNULL(@Duplicados, 0) > 0
    BEGIN
        SET @Msg = N'Claves inválidas en ' + @Origen + N': nulos = '
                 + CAST(ISNULL(@Nulos, 0) AS NVARCHAR(20)) + N', duplicados = '
                 + CAST(ISNULL(@Duplicados, 0) AS NVARCHAR(20)) + N'.';
        THROW 50005, @Msg, 1;
    END;

    -- 4. Construir listas de columnas
    DECLARE @ListaColumnas NVARCHAR(MAX), @ListaValores NVARCHAR(MAX),
            @Asignaciones  NVARCHAR(MAX), @Condicion    NVARCHAR(MAX);

    SELECT
        @ListaColumnas = STRING_AGG(CAST(QUOTENAME(COLUMN_NAME) AS NVARCHAR(MAX)), N', ')
                         WITHIN GROUP (ORDER BY ORDINAL_POSITION),
        @ListaValores  = STRING_AGG(CAST(N'S.' + QUOTENAME(COLUMN_NAME) AS NVARCHAR(MAX)), N', ')
                         WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @EsquemaGold AND TABLE_NAME = @Tabla;

    -- Columnas de negocio: todas menos la clave y las de auditoría
    SELECT
        @Asignaciones = STRING_AGG(CAST(QUOTENAME(COLUMN_NAME) + N' = S.' + QUOTENAME(COLUMN_NAME)
                        AS NVARCHAR(MAX)), N', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION),
        @Condicion    = STRING_AGG(CAST(
                            N'(T.' + QUOTENAME(COLUMN_NAME) + N' <> S.' + QUOTENAME(COLUMN_NAME)
                          + N' OR (T.' + QUOTENAME(COLUMN_NAME) + N' IS NULL AND S.' + QUOTENAME(COLUMN_NAME) + N' IS NOT NULL)'
                          + N' OR (T.' + QUOTENAME(COLUMN_NAME) + N' IS NOT NULL AND S.' + QUOTENAME(COLUMN_NAME) + N' IS NULL))'
                        AS NVARCHAR(MAX)), N' OR ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @EsquemaGold AND TABLE_NAME = @Tabla
      AND LOWER(COLUMN_NAME) NOT IN ('createdat', 'updatedat', 'batchid')
      AND COLUMN_NAME <> @ClaveReal;

    SET @Condicion = ISNULL(@Condicion, N'1 = 0');

    -- CreatedAt no se actualiza; UpdatedAt y BatchId vienen de staging
    SET @Asignaciones = ISNULL(@Asignaciones + N', ', N'')
        + QUOTENAME(@ColUpdatedAt) + N' = S.' + QUOTENAME(@ColUpdatedAt) + N', '
        + QUOTENAME(@ColBatchId)   + N' = S.' + QUOTENAME(@ColBatchId);

    -- 5. Sentencias UPDATE e INSERT
    DECLARE @SqlUpdate NVARCHAR(MAX) =
        N'UPDATE T SET ' + @Asignaciones
      + N' FROM ' + @Destino + N' AS T'
      + N' INNER JOIN ' + @Origen + N' AS S'
      + N' ON T.' + QUOTENAME(@ClaveReal) + N' = S.' + QUOTENAME(@ClaveReal)
      + N' WHERE ' + @Condicion + N'; SET @filas = @@ROWCOUNT;';

    DECLARE @SqlInsert NVARCHAR(MAX) =
        N'INSERT INTO ' + @Destino + N' (' + @ListaColumnas + N')'
      + N' SELECT ' + @ListaValores + N' FROM ' + @Origen + N' AS S'
      + N' WHERE NOT EXISTS (SELECT 1 FROM ' + @Destino + N' AS T'
      + N' WHERE T.' + QUOTENAME(@ClaveReal) + N' = S.' + QUOTENAME(@ClaveReal) + N');'
      + N' SET @filas = @@ROWCOUNT;';

    -- 6. Ejecutar en una transacción
    DECLARE @Actualizadas BIGINT = 0, @Insertadas BIGINT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        EXEC sp_executesql @SqlUpdate, N'@filas BIGINT OUTPUT', @filas = @Actualizadas OUTPUT;
        EXEC sp_executesql @SqlInsert, N'@filas BIGINT OUTPUT', @filas = @Insertadas  OUTPUT;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;

    SELECT @Tabla AS Dimension,
           @Actualizadas AS FilasActualizadas,
           @Insertadas   AS FilasInsertadas;
END;

GO