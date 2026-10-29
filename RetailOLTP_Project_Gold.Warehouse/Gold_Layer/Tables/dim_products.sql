CREATE TABLE [Gold_Layer].[dim_products] (
    [ProductID]       BIGINT        NULL,
    [SKU]             VARCHAR (MAX) NULL,
    [ProductName]     VARCHAR (MAX) NULL,
    [CostPrice]       FLOAT (53)    NULL,
    [UnitsPerPackage] BIGINT        NULL,
    [Status]          VARCHAR (MAX) NULL,
    [CategoryName]    VARCHAR (MAX) NULL,
    [SupplierName]    VARCHAR (MAX) NULL,
    [ContactName]     VARCHAR (MAX) NULL,
    [Email]           VARCHAR (MAX) NULL,
    [Phone]           VARCHAR (MAX) NULL,
    [City]            VARCHAR (MAX) NULL,
    [Country]         VARCHAR (MAX) NULL,
    [CreatedAt]       DATETIME2 (6) NULL,
    [UpdatedAt]       DATETIME2 (6) NULL,
    [BatchID]         VARCHAR (MAX) NULL
);


GO