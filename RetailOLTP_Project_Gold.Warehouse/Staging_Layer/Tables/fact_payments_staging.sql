CREATE TABLE [Staging_Layer].[fact_payments_staging] (
    [PaymentID]     BIGINT          NULL,
    [CustomerID]    BIGINT          NULL,
    [StoreID]       BIGINT          NULL,
    [OrderID]       BIGINT          NULL,
    [PaymentDate]   DATE            NULL,
    [PaymentHour]   VARCHAR (MAX)   NULL,
    [PaymentMethod] VARCHAR (MAX)   NULL,
    [PaymentStatus] VARCHAR (MAX)   NULL,
    [Amount]        DECIMAL (18, 2) NULL,
    [CreatedAt]     DATETIME2 (6)   NOT NULL,
    [UpdatedAt]     DATETIME2 (6)   NOT NULL,
    [BatchID]       VARCHAR (MAX)   NOT NULL
);


GO