CREATE TABLE [Staging_Layer].[dim_employees_staging] (
    [EmployeeID] BIGINT        NULL,
    [FirstName]  VARCHAR (MAX) NULL,
    [LastName]   VARCHAR (MAX) NULL,
    [Email]      VARCHAR (MAX) NULL,
    [JobTitle]   VARCHAR (MAX) NULL,
    [HireDate]   DATE          NULL,
    [Status]     VARCHAR (MAX) NULL,
    [CreatedAt]  DATETIME2 (6) NOT NULL,
    [UpdatedAt]  DATETIME2 (6) NOT NULL,
    [BatchID]    VARCHAR (MAX) NOT NULL
);


GO