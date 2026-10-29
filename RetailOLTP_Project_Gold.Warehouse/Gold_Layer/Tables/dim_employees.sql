CREATE TABLE [Gold_Layer].[dim_employees] (
    [EmployeeID] BIGINT        NULL,
    [FirstName]  VARCHAR (MAX) NULL,
    [LastName]   VARCHAR (MAX) NULL,
    [Email]      VARCHAR (MAX) NULL,
    [JobTitle]   VARCHAR (MAX) NULL,
    [HireDate]   DATE          NULL,
    [Status]     VARCHAR (MAX) NULL,
    [CreatedAt]  DATETIME2 (6) NULL,
    [UpdatedAt]  DATETIME2 (6) NULL,
    [BatchID]    VARCHAR (MAX) NULL
);


GO