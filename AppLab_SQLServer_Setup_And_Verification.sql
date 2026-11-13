CREATE DATABASE AppLabDB;
GO

USE AppLabDB;
GO

--Sales
CREATE TABLE FactSales
(
    OrderID INT NOT NULL,
    CustomerID VARCHAR(50),
    ProductID VARCHAR(50),
    ProductName VARCHAR(200),
    Category VARCHAR(100),
    SaleAmount DECIMAL(18,2),
    Currency VARCHAR(10),
    ExchangeRateToUSD DECIMAL(18,6),
    SaleAmountUSD DECIMAL(18,2),
    OrderDate DATE,
    Discount DECIMAL(10,4),
    Region VARCHAR(100),

    CONSTRAINT PK_FactSales
        PRIMARY KEY (OrderID)
);

--Rejected Records
CREATE TABLE RejectedRecords
(
    OrderID INT,
    CustomerID VARCHAR(50),
    ProductID VARCHAR(50),
    SaleAmount DECIMAL(18,2),
    Currency VARCHAR(10),
    OrderDate VARCHAR(50),
    RejectionReason VARCHAR(1000),
    ErrorTimestamp DATETIME2,
    PipelineName VARCHAR(100)
);

--Error Logs

CREATE TABLE ErrorLogs
(
    ErrorID INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    CustomerID VARCHAR(50),
    ProductID VARCHAR(50),
    ErrorType VARCHAR(200),
    ErrorTimestamp DATETIME2,
    PipelineName VARCHAR(100)
);

----- INSERTING DATA INTO ABOVE TABLES USING pyodbc sqlalchemy

USE AppLabDB;
GO

SELECT
    (SELECT COUNT(*) FROM dbo.FactSales) AS FactSalesCount,
    (SELECT COUNT(*) FROM dbo.RejectedRecords) AS RejectedCount,
    (SELECT COUNT(*) FROM dbo.ErrorLogs) AS ErrorLogCount;

SELECT
    OrderID,
    COUNT(*) AS ErrorCount
FROM dbo.ErrorLogs
GROUP BY OrderID
ORDER BY OrderID;