# Data Analysis on Sales Project with Scala
## Project Overview

This project aims to provide insights into sales data for an e-commerce company. Using Scala, we manage and analyze data on products, orders, customers, and items sold on the platform. The goal is to identify trends, segment customers, and recognize patterns that can drive business decisions.

## Dataset
### 1. Products Data
This dataset contains information about all available products on the platform.
Columns:
product_id: Unique identifier for each product.
category_name: The product's category.
product_name_length, product_description_length, product_photos_qty: Information on the product's content.
product_weight_g, product_length_cm, product_height_cm, product_width_cm: Dimensional details of each product.
### 2. Items Data
This dataset contains details of each item sold on the platform.
Columns:
order_id: Identifier linking each item to its respective order.
order_item_id: Identifier for individual items within an order.
product_id: Links item to product details.
price: Price of the item.
freight_value: Cost of shipping the item.
### 3. Orders Data
This dataset provides details of customer orders.
Columns:
order_id: Unique identifier for each order.
customer_id: Identifier linking the order to a specific customer.
order_status: Current status of the order (e.g., delivered, shipped).
order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date: Various timestamps associated with the order process.
### 4. Customers Data
This dataset provides information about the customers.
Columns:
customer_id: Unique identifier for each customer.
customer_unique_id: Alternate identifier.
customer_zip_code_prefix, customer_city, customer_state: Geographic data of each customer.


## Tools and Technologies Used
---
Scala Language: Used to handle data manipulation, analysis, and processing efficiently.
Functional Programming: Leveraged to manage data immutably and process collections effectively.
Scala Build Tool (SBT): Build automation tool used for compiling, testing, and packaging Scala code.
Process
### Objective 1: Utilizing Scala Collections
Collections Used: Lists and Maps are used to store and manage tables (products, items, orders, and customers).
Centralized Sales View: A centralized view of sales is created for each customer to identify top customers.
Recurring Customers: Analysis to determine customers who have placed more than one order.
### Objective 2: Sales Analysis
Average Basket Value by Category: Calculated average basket value for each product category.
Popular Products: Identified the top 10 most purchased products across the platform.
Recurring Customer Orders: Examined popular items ordered by recurring customers.
### Objective 3: Customer Segmentation
Segmentation Based on Frequency and Spend: Customers are segmented based on their frequency of purchases and total expenditure, categorizing them as Occasional, Frequent, or VIP customers.
## Steps of the Project
![Process Flowchart](image.png) <!
### Data Cleaning:

Removed duplicates, handled missing values, and standardized data entries.
### Data Loading:

Each data file is loaded into Scala collections, making it easy to query and manipulate.
### Sales Analysis:

Centralized view of sales by customer.
Identification of recurring customers.
Calculation of average basket value by category and identification of popular products.
### Customer Segmentation:

Customers are segmented based on their purchasing behavior and overall spending.
## Conclusion
This project leverages Scala’s capabilities to process large datasets and provides essential insights that can help the e-commerce company make informed decisions. By understanding customer behavior, average spending, and popular products, the company can better tailor its strategies to enhance customer experience and optimize sales.

