CREATE TABLE [Gold_Layer].[dim_stores] (
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
    [CreatedAt]   DATETIME2 (6) NULL,
    [UpdatedAt]   DATETIME2 (6) NULL,
    [BatchID]     VARCHAR (MAX) NULL
);


GO