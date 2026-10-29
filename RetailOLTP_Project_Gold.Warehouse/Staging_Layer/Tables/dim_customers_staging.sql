CREATE TABLE [Staging_Layer].[dim_customers_staging] (
    [CustomerID]       BIGINT        NULL,
    [FirstName]        VARCHAR (MAX) NULL,
    [LastName]         VARCHAR (MAX) NULL,
    [Email]            VARCHAR (MAX) NULL,
    [Phone]            VARCHAR (MAX) NULL,
    [BirthDate]        DATE          NULL,
    [Gender]           VARCHAR (MAX) NULL,
    [SegmentName]      VARCHAR (MAX) NULL,
    [AddressLine1]     VARCHAR (MAX) NULL,
    [AddressLine2]     VARCHAR (MAX) NULL,
    [City]             VARCHAR (MAX) NULL,
    [State]            VARCHAR (MAX) NULL,
    [PostalCode]       VARCHAR (MAX) NULL,
    [Country]          VARCHAR (MAX) NULL,
    [RegistrationDate] DATE          NULL,
    [Status]           VARCHAR (MAX) NULL,
    [CreatedAt]        DATETIME2 (6) NOT NULL,
    [UpdatedAt]        DATETIME2 (6) NOT NULL,
    [BatchID]          VARCHAR (MAX) NOT NULL
);


GO