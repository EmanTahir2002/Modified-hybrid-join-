use electronica_dw;

#Querysix

CREATE VIEW STOREANALYSIS_VIEWs AS
SELECT Store_ID,ProductID, ProductName,
    SUM(Quantity) AS TotalQuantitySold,SUM(Amount) AS store_total
FROM Sales_fact 
JOIN Product_dim  ON Product_ID = productID GROUP BY Store_ID, ProductID, ProductName;
SELECT Store_ID, ProductID , store_total FROM STOREANALYSIS_VIEWs;

#query 9
# i am dealing with 2019 only
SELECT CustomerID, CustomerName,
COUNT(DISTINCT Product_ID) AS UniqueProducts, SUM(Amount) AS TotalSales
FROM Sales_fact JOIN customer_dim ON Sales_fact.Customer_ID = customer_dim.customer_key
GROUP BY CustomerID, CustomerName;


#query 8
CREATE VIEW SUPPLIER_PERFORMANCE_MVs AS
SELECT s.supplierID AS Supplier_ID, d.YEAR AS SaleYear, d.MONTH AS SaleMonth,SUM(sf.Amount) AS TotalSales
FROM Sales_fact sf
JOIN supplier_dim s ON sf.Supplier_ID = s.supplier_key
JOIN date_dim d ON sf.Sales_ID = d.Date_key
GROUP BY s.supplierID, SaleYear, SaleMonth;
SELECT * FROM SUPPLIER_PERFORMANCE_MVs;


#query 10
CREATE  VIEW CUSTOMER_STORE_SALES_MV AS
SELECT d.YEAR AS SaleYear,d.MONTH AS SaleMonth, st.storeID AS StoreID,c.CustomerID,SUM(sf.Amount) AS TotalSales
FROM Sales_fact sf
JOIN customer_dim c ON sf.Customer_ID = c.customer_key
JOIN store_dim st ON sf.Store_ID = st.store_key
JOIN date_dim d ON sf.Sales_ID = d.Date_key
GROUP BY SaleYear, SaleMonth, StoreID, c.CustomerID;
select * from CUSTOMER_STORE_SALES_MV
