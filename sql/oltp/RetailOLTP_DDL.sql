USE [RetailOLTP]
GO
/****** Objeto: Table [dbo].[Categories] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Categories](
	[CategoryID] [int] IDENTITY(1,1) NOT NULL,
	[CategoryName] [varchar](100) NULL,
 CONSTRAINT [PK_Categories] PRIMARY KEY CLUSTERED 
(
	[CategoryID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Objeto: Table [dbo].[CustomerAddresses] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CustomerAddresses](
	[CustomerAddressID] [int] IDENTITY(1,1) NOT NULL,
	[CustomerID] [int] NOT NULL,
	[AddressType] [varchar](30) NULL,
	[AddressLine1] [varchar](200) NULL,
	[AddressLine2] [varchar](200) NULL,
	[City] [varchar](100) NULL,
	[State] [varchar](100) NULL,
	[PostalCode] [varchar](20) NULL,
	[Country] [varchar](100) NULL,
	[IsPrimary] [bit] NOT NULL,
 CONSTRAINT [PK_CustomerAddresses] PRIMARY KEY CLUSTERED 
(
	[CustomerAddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Objeto: Table [dbo].[Customers] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Customers](
	[CustomerID] [int] IDENTITY(1,1) NOT NULL,
	[CustomerSegmentID] [int] NULL,
	[FirstName] [varchar](100) NULL,
	[LastName] [varchar](100) NULL,
	[Email] [varchar](150) NULL,
	[Phone] [varchar](30) NULL,
	[BirthDate] [date] NULL,
	[Gender] [varchar](20) NULL,
	[RegistrationDate] [date] NULL,
	[Status] [varchar](30) NULL,
 CONSTRAINT [PK_Customers] PRIMARY KEY CLUSTERED 
(
	[CustomerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Objeto: Table [dbo].[CustomerSegments] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CustomerSegments](
	[CustomerSegmentID] [int] IDENTITY(1,1) NOT NULL,
	[SegmentName] [varchar](100) NULL,
 CONSTRAINT [PK_CustomerSegments] PRIMARY KEY CLUSTERED 
(
	[CustomerSegmentID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Objeto: Table [dbo].[Employees] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Employees](
	[EmployeeID] [int] IDENTITY(1,1) NOT NULL,
	[StoreID] [int] NULL,
	[FirstName] [varchar](100) NULL,
	[LastName] [varchar](100) NULL,
	[Email] [varchar](150) NULL,
	[JobTitle] [varchar](100) NULL,
	[HireDate] [date] NULL,
	[Status] [varchar](30) NULL,
 CONSTRAINT [PK_Employees] PRIMARY KEY CLUSTERED 
(
	[EmployeeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Objeto: Table [dbo].[OrderDetails] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[OrderDetails](
	[OrderDetailID] [bigint] IDENTITY(1,1) NOT NULL,
	[OrderID] [bigint] NULL,
	[ProductID] [int] NULL,
	[Quantity] [int] NULL,
	[UnitPrice] [decimal](18, 2) NULL,
	[DiscountPercent] [decimal](5, 2) NULL,
	[DiscountAmount] [decimal](18, 2) NULL,
	[TaxAmount] [decimal](18, 2) NULL,
	[LineTotal] [decimal](18, 2) NULL,
 CONSTRAINT [PK_OrderDetails] PRIMARY KEY CLUSTERED 
(
	[OrderDetailID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Objeto: Table [dbo].[Orders] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Orders](
	[OrderID] [bigint] IDENTITY(1,1) NOT NULL,
	[CustomerID] [int] NULL,
	[StoreID] [int] NULL,
	[EmployeeID] [int] NULL,
	[OrderDate] [datetime2](0) NULL,
	[OrderStatus] [varchar](30) NULL,
	[Subtotal] [decimal](18, 2) NULL,
	[DiscountAmount] [decimal](18, 2) NULL,
	[TaxAmount] [decimal](18, 2) NULL,
	[ShippingAmount] [decimal](18, 2) NULL,
	[TotalAmount] [decimal](18, 2) NULL,
 CONSTRAINT [PK_Orders] PRIMARY KEY CLUSTERED 
(
	[OrderID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Objeto: Table [dbo].[Payments] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Payments](
	[PaymentID] [bigint] IDENTITY(1,1) NOT NULL,
	[OrderID] [bigint] NULL,
	[PaymentDate] [datetime2](0) NULL,
	[PaymentMethod] [varchar](50) NULL,
	[PaymentStatus] [varchar](30) NULL,
	[Amount] [decimal](18, 2) NULL,
	[TransactionReference] [varchar](100) NULL,
 CONSTRAINT [PK_Payments] PRIMARY KEY CLUSTERED 
(
	[PaymentID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Objeto: Table [dbo].[Products] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Products](
	[ProductID] [int] IDENTITY(1,1) NOT NULL,
	[CategoryID] [int] NULL,
	[SupplierID] [int] NULL,
	[SKU] [varchar](50) NULL,
	[ProductName] [varchar](150) NULL,
	[UnitPrice] [decimal](18, 2) NULL,
	[CostPrice] [decimal](18, 2) NULL,
	[UnitsPerPackage] [int] NULL,
	[Status] [varchar](30) NULL,
 CONSTRAINT [PK_Products] PRIMARY KEY CLUSTERED 
(
	[ProductID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Objeto: Table [dbo].[Returns] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Returns](
	[ReturnID] [bigint] IDENTITY(1,1) NOT NULL,
	[OrderDetailID] [bigint] NULL,
	[ReturnDate] [datetime2](0) NULL,
	[Quantity] [int] NULL,
	[ReturnReason] [varchar](100) NULL,
	[RefundAmount] [decimal](18, 2) NULL,
	[ReturnStatus] [varchar](30) NULL,
 CONSTRAINT [PK_Returns] PRIMARY KEY CLUSTERED 
(
	[ReturnID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Objeto: Table [dbo].[Stores] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Stores](
	[StoreID] [int] IDENTITY(1,1) NOT NULL,
	[StoreCode] [varchar](20) NULL,
	[StoreName] [varchar](100) NULL,
	[Address] [varchar](200) NULL,
	[City] [varchar](100) NULL,
	[State] [varchar](100) NULL,
	[Country] [varchar](100) NULL,
	[OpeningDate] [date] NULL,
	[StoreType] [varchar](30) NULL,
	[Status] [varchar](30) NULL,
 CONSTRAINT [PK_Stores] PRIMARY KEY CLUSTERED 
(
	[StoreID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Objeto: Table [dbo].[Suppliers] Fecha de script: 1/11/2026 18:34:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Suppliers](
	[SupplierID] [int] IDENTITY(1,1) NOT NULL,
	[SupplierName] [varchar](150) NULL,
	[ContactName] [varchar](150) NULL,
	[Email] [varchar](150) NULL,
	[Phone] [varchar](30) NULL,
	[City] [varchar](100) NULL,
	[Country] [varchar](100) NULL,
 CONSTRAINT [PK_Suppliers] PRIMARY KEY CLUSTERED 
(
	[SupplierID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[CustomerAddresses]  WITH CHECK ADD  CONSTRAINT [FK_CustomerAddresses_Customers] FOREIGN KEY([CustomerID])
REFERENCES [dbo].[Customers] ([CustomerID])
GO
ALTER TABLE [dbo].[CustomerAddresses] CHECK CONSTRAINT [FK_CustomerAddresses_Customers]
GO
ALTER TABLE [dbo].[Customers]  WITH CHECK ADD  CONSTRAINT [FK_Customers_CustomerSegments] FOREIGN KEY([CustomerSegmentID])
REFERENCES [dbo].[CustomerSegments] ([CustomerSegmentID])
GO
ALTER TABLE [dbo].[Customers] CHECK CONSTRAINT [FK_Customers_CustomerSegments]
GO
ALTER TABLE [dbo].[Employees]  WITH CHECK ADD  CONSTRAINT [FK_Employees_Stores] FOREIGN KEY([StoreID])
REFERENCES [dbo].[Stores] ([StoreID])
GO
ALTER TABLE [dbo].[Employees] CHECK CONSTRAINT [FK_Employees_Stores]
GO
ALTER TABLE [dbo].[OrderDetails]  WITH CHECK ADD  CONSTRAINT [FK_OrderDetails_Orders] FOREIGN KEY([OrderID])
REFERENCES [dbo].[Orders] ([OrderID])
GO
ALTER TABLE [dbo].[OrderDetails] CHECK CONSTRAINT [FK_OrderDetails_Orders]
GO
ALTER TABLE [dbo].[OrderDetails]  WITH CHECK ADD  CONSTRAINT [FK_OrderDetails_Products] FOREIGN KEY([ProductID])
REFERENCES [dbo].[Products] ([ProductID])
GO
ALTER TABLE [dbo].[OrderDetails] CHECK CONSTRAINT [FK_OrderDetails_Products]
GO
ALTER TABLE [dbo].[Orders]  WITH CHECK ADD  CONSTRAINT [FK_Orders_Customers] FOREIGN KEY([CustomerID])
REFERENCES [dbo].[Customers] ([CustomerID])
GO
ALTER TABLE [dbo].[Orders] CHECK CONSTRAINT [FK_Orders_Customers]
GO
ALTER TABLE [dbo].[Orders]  WITH CHECK ADD  CONSTRAINT [FK_Orders_Employees] FOREIGN KEY([EmployeeID])
REFERENCES [dbo].[Employees] ([EmployeeID])
GO
ALTER TABLE [dbo].[Orders] CHECK CONSTRAINT [FK_Orders_Employees]
GO
ALTER TABLE [dbo].[Orders]  WITH CHECK ADD  CONSTRAINT [FK_Orders_Stores] FOREIGN KEY([StoreID])
REFERENCES [dbo].[Stores] ([StoreID])
GO
ALTER TABLE [dbo].[Orders] CHECK CONSTRAINT [FK_Orders_Stores]
GO
ALTER TABLE [dbo].[Payments]  WITH CHECK ADD  CONSTRAINT [FK_Payments_Orders] FOREIGN KEY([OrderID])
REFERENCES [dbo].[Orders] ([OrderID])
GO
ALTER TABLE [dbo].[Payments] CHECK CONSTRAINT [FK_Payments_Orders]
GO
ALTER TABLE [dbo].[Products]  WITH CHECK ADD  CONSTRAINT [FK_Products_Categories] FOREIGN KEY([CategoryID])
REFERENCES [dbo].[Categories] ([CategoryID])
GO
ALTER TABLE [dbo].[Products] CHECK CONSTRAINT [FK_Products_Categories]
GO
ALTER TABLE [dbo].[Products]  WITH CHECK ADD  CONSTRAINT [FK_Products_Suppliers] FOREIGN KEY([SupplierID])
REFERENCES [dbo].[Suppliers] ([SupplierID])
GO
ALTER TABLE [dbo].[Products] CHECK CONSTRAINT [FK_Products_Suppliers]
GO
ALTER TABLE [dbo].[Returns]  WITH CHECK ADD  CONSTRAINT [FK_Returns_OrderDetails] FOREIGN KEY([OrderDetailID])
REFERENCES [dbo].[OrderDetails] ([OrderDetailID])
GO
ALTER TABLE [dbo].[Returns] CHECK CONSTRAINT [FK_Returns_OrderDetails]
GO

