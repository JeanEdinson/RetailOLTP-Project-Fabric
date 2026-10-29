CREATE TABLE [Staging_Layer].[dim_stores_staging] (
    [StoreID]     BIGINT        NULL,
    [StoreCode]   VARCHAR (MAX) NULL,
    [StoreName]   VARCHAR (MAX) NULL,
    [Address]     VARCHAR (MAX) NULL,
    [City]        VARCHAR (MAX) NULL,
    [State]       VARCHAR (MAX) NULL,
    [Country]     VARCHAR (MAX) NULL,
    [OpeningDate] DATE          NULL,
    [StoreType]   VARCHAR (MAX) NULL,
    [Status]      VARCHAR (MAX) NULL,
    [CreatedAt]   DATETIME2 (6) NOT NULL,
    [UpdatedAt]   DATETIME2 (6) NOT NULL,
    [BatchID]     VARCHAR (MAX) NOT NULL
);


GO