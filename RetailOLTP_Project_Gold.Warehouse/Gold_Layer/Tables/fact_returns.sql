CREATE TABLE [Gold_Layer].[fact_returns] (
    [ReturnID]      BIGINT          NULL,
    [CustomerID]    BIGINT          NULL,
    [ProductID]     INT             NULL,
    [StoreID]       BIGINT          NULL,
    [OrderDetailID] BIGINT          NULL,
    [ReturnDate]    DATE            NULL,
    [ReturnHour]    VARCHAR (MAX)   NULL,
    [Quantity]      INT             NULL,
    [ReturnReason]  VARCHAR (MAX)   NULL,
    [RefundAmount]  DECIMAL (18, 2) NULL,
    [ReturnStatus]  VARCHAR (MAX)   NULL,
    [CreatedAt]     DATETIME2 (6)   NOT NULL,
    [UpdatedAt]     DATETIME2 (6)   NOT NULL,
    [BatchID]       VARCHAR (MAX)   NOT NULL
);


GO