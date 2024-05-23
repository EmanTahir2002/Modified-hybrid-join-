			
				#dimension tables 
create database ELECTRONICA_DW;
use ELECTRONICA_DW;


-- Create Supplier Dimension Table
DROP TABLE IF EXISTS supplier_dim;
CREATE TABLE supplier_dim (
	supplier_key INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
    supplierID INT NOT NULL,
    supplierName VARCHAR(255) NOT NULL
);

-- Create Store Dimension Table
DROP TABLE IF EXISTS store_dim;
CREATE TABLE store_dim (
	store_key INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    storeID INT NOT NULL ,
    storeName VARCHAR(255) NOT NULL
);

-- Create Product Dimension Table
DROP TABLE IF EXISTS product_dim;
CREATE TABLE product_dim (
	product_key INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    productID INT NOT NULL,
    productName VARCHAR(255) NOT NULL ,
    productPrice DECIMAL(10,2) NOT NULL
);

-- Create Customer Dimension Table
DROP TABLE IF EXISTS customer_dim;
CREATE TABLE customer_dim (
	customer_key INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    CustomerID INT NOT NULL,
    CustomerName VARCHAR(255) NOT NULL,
    Gender CHAR(255)
);

-- Create Date Dimension Table
DROP TABLE IF EXISTS date_dim;
CREATE TABLE date_dim (
	Date_key int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    DateID int ,
    MONTH INT,
    YEAR INT
);

#fact table
DROP TABLE IF EXISTS Sales_fact;
CREATE TABLE Sales_fact (
    Sales_ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Customer_ID INT,
    Supplier_ID INT,
    Store_ID INT,
    Product_ID INT,
    Quantity INT,
    Amount DECIMAL(10,2),
    FOREIGN KEY (Customer_ID) REFERENCES Customer_dim(customer_key),
    FOREIGN KEY (Supplier_ID) REFERENCES supplier_dim(supplier_key),
    FOREIGN KEY (Store_ID) REFERENCES store_dim(store_key),
    FOREIGN KEY (Product_ID) REFERENCES product_dim(product_key)
);

select * from date_dim;

select * from product_dim ;
select * from customer_dim ;
select * from supplier_dim ;
select * from store_dim ;
select* from Sales_fact;




