-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Incremental Data Loading

-- COMMAND ----------



-- COMMAND ----------

CREATE DATABASE Sale_scd;

-- COMMAND ----------

CREATE TABLE sale_scd.Orders (
    OrderID INT,
    OrderDate DATE,
    CustomerID INT,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    ProductID INT,
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(50),
    RegionID INT,
    RegionName VARCHAR(50),
    Country VARCHAR(50),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalAmount DECIMAL(10,2)
);

-- COMMAND ----------

delete from sale_scd.Orders

-- COMMAND ----------

INSERT INTO sale_scd.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
VALUES 
(1, '2024-02-01', 101, 'Alice Johnson', 'alice@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
(2, '2024-02-02', 102, 'Bob Smith', 'bob@example.com', 202, 'Smartphone', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00),
(3, '2024-02-03', 103, 'Charlie Brown', 'charlie@example.com', 203, 'Tablet', 'Electronics', 303, 'Asia', 'India', 3, 300.00, 900.00),
(4, '2024-02-04', 101, 'Alice Johnson', 'alice@example.com', 204, 'Headphones', 'Accessories', 301, 'North America', 'USA', 1, 150.00, 150.00),
(5, '2024-02-05', 104, 'David Lee', 'david@example.com', 205, 'Gaming Console', 'Electronics', 302, 'Europe', 'France', 1, 400.00, 400.00),
(6, '2024-02-06', 102, 'Bob Smith', 'bob@example.com', 206, 'Smartwatch', 'Electronics', 303, 'Asia', 'China', 2, 200.00, 400.00),
(7, '2024-02-07', 105, 'Eve Adams', 'eve@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'Canada', 1, 800.00, 800.00),
(8, '2024-02-08', 106, 'Frank Miller', 'frank@example.com', 207, 'Monitor', 'Accessories', 302, 'Europe', 'Italy', 2, 250.00, 500.00),
(9, '2024-02-09', 107, 'Grace White', 'grace@example.com', 208, 'Keyboard', 'Accessories', 303, 'Asia', 'Japan', 3, 100.00, 300.00),
(10, '2024-02-10', 104, 'David Lee', 'david@example.com', 209, 'Mouse', 'Accessories', 301, 'North America', 'USA', 1, 50.00, 50.00);


-- COMMAND ----------

INSERT INTO sales.Orders (
    OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail,
    ProductID, ProductName, ProductCategory,
    RegionID, RegionName, Country,
    Quantity, UnitPrice, TotalAmount
)
VALUES
(11, '2024-02-11', 108, 'Hannah Green', 'hannah@example.com', 210, 'Wireless Earbuds', 'Accessories', 302, 'Europe', 'Spain', 2, 120.00, 240.00),

(12, '2024-02-12', 109, 'Ian Black', 'ian@example.com', 201, 'Laptop', 'Electronics', 303, 'Asia', 'India', 1, 800.00, 800.00),

(13, '2024-02-13', 105, 'Eve Adams', 'eve@example.com', 202, 'Smartphone', 'Electronics', 301, 'North America', 'Canada', 1, 500.00, 500.00),

(14, '2024-02-14', 110, 'Jack Wilson', 'jack@example.com', 211, 'External Hard Drive', 'Accessories', 302, 'Europe', 'UK', 2, 150.00, 300.00),

(15, '2024-02-15', 101, 'Alice Johnson', 'alice@example.com', 203, 'Tablet', 'Electronics', 301, 'North America', 'USA', 1, 300.00, 300.00);

-- COMMAND ----------

select * from sales_new.orders 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DATA WAREHOUSING

-- COMMAND ----------

CREATE DATABASE ordersDWH

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Staging Layer

-- COMMAND ----------

-- initial load
CREATE or Replace TABLE ordersDWH.stg_sales
as 
select * from sales_new.orders

-- COMMAND ----------

select * from ordersDWH.stg_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transformation

-- COMMAND ----------

CREATE VIEW ordersDWH.trans_sales
AS
select * from ordersDWH.stg_sales
where Quantity is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Core Layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DimCustomer

-- COMMAND ----------

CREATE OR REPLACE TABLE ordersDWH.DimCustomer
(
  CustomerID int,
  CustomerName string,
  CustomerEmail string,
  DimCustomerKey int
)

-- COMMAND ----------

select T.*,row_number() over(order by CustomerID) as DimCustomerKey from 
(
select 
    distinct CustomerID,
    CustomerName,
    CustomerEmail
              from ordersDWH.trans_sales) as T

-- COMMAND ----------

drop view ordersDWH.View_DimCustomers

-- COMMAND ----------

CREATE VIEW ordersDWH.View_DimCustomers
AS

select T.*,row_number() over(order by CustomerID) as DimCustomerKey from 
(
select 
    distinct CustomerID,
    CustomerName,
    CustomerEmail
              from ordersDWH.trans_sales) as T

-- COMMAND ----------

select * from ordersDWH.View_DimCustomers

-- COMMAND ----------

INSERT INTO ordersDWH.DimCustomer
SELECT * FROM ordersDWH.View_DimCustomers

-- COMMAND ----------

select * from ordersDWH.DimCustomer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DimProduct

-- COMMAND ----------

CREATE OR REPLACE TABLE ordersDWH.DimProducts
(
  ProductID int,
  ProductName string,
  ProductCategory string,
  DimProductKey int
)

-- COMMAND ----------

select T.*,row_number() over(order by ProductID) as DimProductKey from 
(
select 
    distinct ProductID,
    ProductName,
    ProductCategory
              from ordersDWH.trans_sales) as T

-- COMMAND ----------

CREATE VIEW ordersDWH.View_DimProducts
AS
(select T.*,row_number() over(order byProductID) as DimProductKey
   from 
        (
        select 
            distinct ProductID,
            ProductName,
            ProductCategory
                      from ordersDWH.trans_sales) as T)

-- COMMAND ----------

select * from ordersDWH.View_DimProducts

-- COMMAND ----------

insert into ordersDWH.DimProducts
select * from ordersDWH.View_DimProducts

-- COMMAND ----------

select * from ordersDWH.DimProducts

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DimRegion

-- COMMAND ----------

CREATE OR REPLACE TABLE ordersDWH.DimRegion
(
  RegionID int,
  RegionName string,
  Country string,
  DimRegionKey int
)

-- COMMAND ----------

CREATE VIEW ordersDWH.View_DimRegion
AS
select T.*,row_number() over(order by RegionID) as DimRegionKey from 
(
select 
    distinct RegionID,
    RegionName,
    Country
              from ordersDWH.trans_sales) as T

-- COMMAND ----------

select * from ordersDWH.View_DimRegion

-- COMMAND ----------

insert into ordersDWH.DimRegion
select * from ordersDWH.View_DimRegion

-- COMMAND ----------

select * from ordersDWH.DimRegion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DimDate

-- COMMAND ----------

CREATE OR REPLACE TABLE ordersDWH.DimDate
(
 OrderDate DATE,
 DimDateKey int
)

-- COMMAND ----------

CREATE VIEW ordersDWH.View_DimDate
AS
select T.*,row_number() over(order by OrderDate) as DimDateKey from 
(
select 
    distinct(OrderDate) as OrderDate
              from ordersDWH.trans_sales) as T

-- COMMAND ----------

select * from ordersDWH.view_dimdate 

-- COMMAND ----------

insert into ordersDWH.DimDate
select * from ordersDWH.View_DimDate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Fact Table

-- COMMAND ----------

CREATE TABLE ordersDWH.FactSales
(
  OrderID int,
  Quantity int,
  UnitPrice DECIMAL,
  TotalAmount DECIMAL,
  DimCustomerKey int,
  DimDateKey int,
  DimProductKey int,
  DimRegionKey int
  
)


-- COMMAND ----------

select 
  F.Quantity,
  F.UnitPrice,
  F.TotalAmount,
  DC.DimCustomerKey,
  DP.DimProductKey,
  DR.DimRegionKey,
  DD.DimDateKey
  from ordersDWH.trans_sales F
  Left join
   ordersDWH.DimCustomer DC
  on
   F.CustomerID = DC.CustomerID
  Left Join ordersDWH.DimProducts DP
  on
   F.ProductID = DP.ProductID
  Left Join ordersDWH.DimRegion DR
  on
   F.Country = DR.Country
  Left Join ordersDWH.DimDate DD
  on
   F.OrderDate = DD.OrderDate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SCD Type - 1

-- COMMAND ----------

select * from sale_scd.orders

-- COMMAND ----------

CREATE OR REPLACE VIEW sale_scd.View_DimProducts
AS
select 
  distinct productID,ProductName,ProductCategory 
     from sale_scd.orders
     where OrderDate > '2024-02-10'
    

-- COMMAND ----------

CREATE OR REPLACE TABLE Sale_scd.DimProducts
(
  ProductID int,
  ProductName string,
  ProductCategory string
)

-- COMMAND ----------

select * from sale_scd.View_DimProducts

-- COMMAND ----------

insert into Sale_scd.DimProducts
select * from sale_scd.View_DimProducts

-- COMMAND ----------

INSERT INTO sale_scd.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
VALUES 
(1, '2024-02-11', 101, 'Alice Johnson', 'alice@example.com', 201, 'gaming Laptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
(2, '2024-02-12', 102, 'Bob Smith', 'bob@example.com', 230, 'Airpodes', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00)

-- COMMAND ----------

select * from sale_scd.View_DimProducts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## MERGE - SCD TYPE 1

-- COMMAND ----------

MERGE INTO sale_scd.DimProducts as trg
USING sale_scd.View_DimProducts as src
ON trg.ProductID = src.ProductID
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN
INSERT *

-- COMMAND ----------

select * from Sale_scd.DimProducts

-- COMMAND ----------

