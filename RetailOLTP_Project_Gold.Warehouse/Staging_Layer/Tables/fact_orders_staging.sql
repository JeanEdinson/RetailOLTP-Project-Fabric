CREATE TABLE [Staging_Layer].[fact_orders_staging] (
    [OrderID]         BIGINT          NULL,
    [OrderDetailID]   BIGINT          NULL,
    [CustomerID]      BIGINT          NULL,
    [ProductID]       INT             NULL,
    [StoreID]         BIGINT          NULL,
    [EmployeeID]      BIGINT          NULL,
    [OrderDate]       DATE            NULL,
    [OrderHour]       VARCHAR (MAX)   NULL,
    [OrderStatus]     VARCHAR (MAX)   NULL,
    [Quantity]        INT             NULL,
    [UnitPrice]       DECIMAL (18, 2) NULL,
    [DiscountPercent] DECIMAL (14, 2) NULL,
    [DiscountAmount]  DECIMAL (18, 2) NULL,
    [TaxAmount]       DECIMAL (18, 2) NULL,
    [ShippingAmount]  DECIMAL (18, 2) NULL,
    [LineTotal]       DECIMAL (32, 2) NULL,
    [CreatedAt]       DATETIME2 (6)   NOT NULL,
    [UpdatedAt]       DATETIME2 (6)   NOT NULL,
    [BatchID]         VARCHAR (MAX)   NOT NULL
);


GO