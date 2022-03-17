/*  1 - This script was created to run on a Microsoft SQL Server DBMS, version: Microsoft SQL Server 2019 (RTM) - 15.0.2000.5 (X64)
    2 - In this script the tables will be created, the csv files will be imported and the necessary queries will be made.
*/

/* Creating the database */

CREATE DATABASE Your-DataBase-Name;

/* Selecting the created database for the following operations to be performed on it */

USE db_cornershop;

/* Creating tables to import with data from .csv files */

/* Creating the tbl_orders table. This table needed to have the column name "status" modified as it is a reserved word in SQLServer */

CREATE TABLE tbl_orders (
   order_id VARCHAR(50) NOT NULL PRIMARY KEY,
   order_status VARCHAR(10),
   created_dow INT,
   created_time TIME,
   updated_dow INT,
   updated_time TIME,
   delivered_dow INT,
   delivered_time TIME,
   picker_id VARCHAR(50),
   driver_id VARCHAR(50),
   storebranch_id VARCHAR(50)
 );

/* Creating the tbl_orders_product table*/

CREATE TABLE tbl_orders_product (
   order_id VARCHAR(50) NOT NULL,
   product_id  VARCHAR(50) NOT NULL,
   quantity FLOAT,
   quantity_found FLOAT,
   unit_price FLOAT
 );

/* Creating the tbl_store_branch table.*/

CREATE TABLE tbl_store_branch (
   storebranch_id VARCHAR(50) NOT NULL PRIMARY KEY,
   store VARCHAR(50) NOT NULL,
   city VARCHAR(50) NOT NULL
 );

/* Creating the tbl_shoppers table.*/

CREATE TABLE tbl_shoppers (
   shopper_id VARCHAR(50) NOT NULL PRIMARY KEY,
   seniority VARCHAR(20) ,
   found_rate FLOAT,
   picking_speed FLOAT,
   accepted_rate FLOAT,
   rating FLOAT
 );


/* Importing data from .csv files into db_cornershop database tables */

/* Importing the data into the tbl_orders table */

BULK INSERT tbl_orders
FROM 'orders.csv'
WITH
(
    FIRSTROW = 2, -- as 1st one is header
    FIELDTERMINATOR = ',',  --CSV field delimiter
    ROWTERMINATOR = '\n',   --Use to shift the control to next row
    TABLOCK
);

/* Query to certify that data has been loaded */

SELECT TOP 5 * FROM tbl_orders;

/* Importing the data into the tbl_orders_product table */

BULK INSERT tbl_orders_product
FROM 'orders_product.csv'
WITH
(
    FIRSTROW = 2, -- as 1st one is header
    FIELDTERMINATOR = ',',  --CSV field delimiter
    ROWTERMINATOR = '\n',   --Use to shift the control to next row
    TABLOCK
);

/* Query to certify that data has been loaded */

SELECT TOP 5 * FROM tbl_orders_product;

/* Importing the data into the tbl_store_branch table */

BULK INSERT tbl_store_branch
FROM 'store_branch.csv'
WITH
(
    FIRSTROW = 2, -- as 1st one is header
    FIELDTERMINATOR = ',',  --CSV field delimiter
    ROWTERMINATOR = '\n',   --Use to shift the control to next row
    TABLOCK
);

/* Query to certify that data has been loaded */

SELECT TOP 5 * FROM tbl_store_branch;

/* Importing data into tbl_shoppers table */

BULK INSERT tbl_shoppers
FROM 'shoppers.csv'
WITH
(
    FIRSTROW = 2, -- as 1st one is header
    FIELDTERMINATOR = ',',  --CSV field delimiter
    ROWTERMINATOR = '\n',   --Use to shift the control to next row
    TABLOCK
);

/* Query to certify that data has been loaded */

SELECT TOP 5 * FROM tbl_shoppers;

/* Now, with all the tables created and their data properly
    loaded we will make the queries to answer the questions of the case */

/* (1) Calculate the number of orders per day of the week, distinguishing their status. */

SELECT created_dow, order_status, COUNT(order_id) AS qtd_orders FROM tbl_orders GROUP BY created_dow, order_status ORDER BY created_dow ASC;

/* (2) Calculate the cancellation rate grouped by each store. */

SELECT
d3.store,
d3.order_status,
d3.quantity,
CAST(d3.quantity AS NUMERIC(15,3)) / CAST(d3.total AS NUMERIC(15,3)) * 100.0 AS cancelation_rate
FROM (
	SELECT
	d2.store,
	d2.order_status,
	d2.quantity,
	d2.total
	FROM (
		SELECT
			d1.store,
			d1.order_status,
			d1.quantity,
			CAST(SUM(d1.quantity) OVER() AS NUMERIC(15,3)) total
			FROM (
				SELECT
					d.store,
					d.order_status,
					COUNT(d.order_status) AS quantity
						FROM (
						SELECT store, order_id, order_status FROM tbl_orders
						INNER JOIN tbl_store_branch ON tbl_store_branch.storebranch_id = tbl_orders.storebranch_id
						) d GROUP BY d.store, d.order_status) d1)d2 WHERE d2.order_status = 'CANCELED') d3 ORDER BY d3.store;


/* (3) Calculate the average found rate(*) of the orders grouped by store and city */

SELECT
	d2.store,
	d2.city,
	d2.found,
	ROUND(CAST(d2.times_found AS NUMERIC(15,2)) / CAST(SUM(d2.times_found) 
	OVER() AS NUMERIC (15,2)), 2) avg_found_rate
FROM(
	SELECT
			d1.store,
			d1.city,
			d1.found,
			COUNT(d1.found) times_found
	FROM(
			SELECT
			d.order_id,
			d.store,
			d.city,
			d.product_id,
			d.quantity_found,

			CASE
				WHEN d.quantity_found > 0 THEN 'yes'
				ELSE 'no'
			END found
				FROM (
					SELECT tbl_orders.order_id, store, city, product_id, quantity_found FROM tbl_orders
					INNER JOIN tbl_store_branch ON tbl_orders.storebranch_id = tbl_store_branch.storebranch_id 
					INNER JOIN tbl_orders_product ON tbl_orders_product.order_id = tbl_orders.order_id)
					d) d1 GROUP BY d1.store, d1.city,d1.found) d2;



/* (4) Determine top 3 selling products ids in total volume delivered to customer */

SELECT TOP 3 product_id, SUM(CAST(quantity AS FLOAT)) AS total_quantity 
FROM tbl_orders_product 
GROUP BY product_id 
ORDER BY total_quantity DESC;

/* (5) Calculate the % of delivered orders that were created and delivered in different days, grouped by hour of creation */

SELECT
d3.same_day,
d3.created_hour,
d3.qtd,
d3.total,
(CAST(d3.qtd AS NUMERIC (15, 3)) / CAST(d3.total AS NUMERIC (15, 3))) * 100.0 AS perc
FROM (
	SELECT
		d2.same_day,
		d2.created_hour,
		d2.qtd,
		SUM(d2.qtd) OVER() AS total
	FROM (
		SELECT
		d1.same_day,
		d1.created_hour,
		COUNT(d1.same_day) AS qtd
		FROM(
			SELECT
			d.order_id,
			d.created_time,
			d.created_dow,
			d.delivered_dow,
			LEFT(d.created_time, 2) AS created_hour,
			CASE
				WHEN d.created_dow <> d.delivered_dow THEN 'yes'
				ELSE 'no'
			END same_day
				FROM(
				SELECT order_id, created_time, created_dow, delivered_dow
				FROM tbl_orders WHERE order_status = 'DELIVERED') d) d1 GROUP BY d1.same_day, d1.created_hour)d2 )d3;

/* (6) Calculate how many products there are in each partition of the ABC Sales Curve(**) grouped by store */

SELECT d3.store,
	   d3.class_abc,
	   COUNT(d3.class_abc) AS quatity_abc
FROM (
SELECT d2.store,
	   d2.product_id,
	   d2.total_billing,
	   d2.grand_total,
	   d2.perc,
	   d2.acc_perc,
	   CASE
			WHEN d2.acc_perc <= 80 THEN 'A'
			WHEN d2.acc_perc <= 95 THEN 'B'
			ELSE 'C'
		END class_abc
FROM (
SELECT d1.store,
	   d1.product_id,
	   d1.total_billing,
	   d1.grand_total,
	   d1.perc,
	   SUM(d1.perc) OVER(ORDER BY d1.total_billing DESC) acc_perc

FROM (
SELECT d.store,
	d.product_id,
	d.total_billing,
	SUM(d.total_billing) OVER() grand_total,
	CAST(d.total_billing AS NUMERIC(15,3)) / CAST(SUM(d.total_billing) OVER() AS NUMERIC(15,3)) * 100 perc
		FROM (
			SELECT store, product_id, SUM(CAST(quantity AS FLOAT))* sum(unit_price) AS total_billing FROM tbl_orders
			INNER JOIN tbl_store_branch ON tbl_orders.storebranch_id = tbl_store_branch.storebranch_id 
			INNER JOIN tbl_orders_product ON tbl_orders_product.order_id = tbl_orders.order_id
			GROUP BY store, product_id) d) d1) d2)d3 group by  d3.store, d3.class_abc ORDER BY d3.store, d3.class_abc;


/* (7) Calculate the % of delivered orders in which the picker_id and driver_id are different. */

SELECT
	d2.class,
	d2.quantity,
	ROUND(CAST(d2.quantity AS NUMERIC(15,2)) / CAST(SUM(d2.quantity) OVER() AS NUMERIC (15,2))*100, 2) percent_
		FROM (

		SELECT
		d1.class,
		COUNT(d1.class) quantity
			FROM (

			SELECT
			d.order_id,
			CASE
				WHEN d.picker_id <> d.driver_id THEN 'different'
				ELSE 'equal'
			END class

				FROM (
				SELECT tbl_orders.picker_id, tbl_orders.driver_id, tbl_orders.order_id
				FROM tbl_orders
				) d) d1 GROUP BY d1.class) d2;
