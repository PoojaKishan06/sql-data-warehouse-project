# Data Catalog for Gold Layer

## Overview
The Gold Layer represents business-level data, organized to support analytics and reporting. It includes **dimension tables** and **fact tables** focused on key business metrics.
---

## 1. **gold_dim_customers**
- **Purpose:** Contains customer information enhanced with demographic and geographic attributes.
- **Columns:**

|Column Name       |Data Type     |Description                                                                           |
|------------------|--------------|--------------------------------------------------------------------------------------|
|customer_key      |INT           |Surrogate key that uniquely identifies each customer entry in the dimension table.    |
|customer_id       |INT           |Unique numerical identifier assigned to each customer.                                |
|customer_number   |VARCHAR(50)   |Alphanumeric identifier representing the customer (used for tracking and referencing).|
|first_name        |VARCHAR(50)   |Customer's first name (as recorded in system).                                        |
|last_name         |VARCHAR(50)   |Customer's last name or family name.                                                  |
|country           |VARCHAR(50)   |Customer's country of residence (e.g., 'Germany').                                    |
|marital_status    |VARCHAR(50)   |Marital status of the customer (e.g., 'Married','Single').                            |
|gender            |VARCHAR(50)   |Gender of the customer (e.g., 'Female', 'Male', 'n/a').                               |
|birth_date        |DATE          |Date of Birth of the customer, formatted as 'YYYY-MM-DD' (e.g., 1971-11-10).          |
|create_date       |DATE          |Date and Time when the customer record was created in the system.                     |

---

## 2. **gold_dim_products**
- **Purpose:** Provides information about the products and it's attributes.
- **Columns:**

|Column Name      |Data Type     |Description                                                                                 |
|-----------------|--------------|--------------------------------------------------------------------------------------------|
|product_key      |INT           |Surrogate key that uniquely identifies each product entry in the product dimension table.   |
|product_id       |INT           |Unique numerical identifier assigned to product for internal tracking and referencing.      |
|product_number   |VARCHAR(50)   |Alphanumeric identifier representing the product (used for categorization and inventory).   |
|product_name     |VARCHAR(50)   |Descriptive name of the product, including key details such as type, color and size.        |
|category_id      |VARCHAR(50)   |Unique identifier of the product's category, linking to it's high-level classification.     |
|category         |VARCHAR(50)   |Broader classification of product to group related items (e.g., Bikes, Components).         |
|subcategory      |VARCHAR(50)   |More detailed classification of the product within the category,such as product type.       |
|maintenance      |VARCHAR(50)   |Indicates whether the product need maintenance (e.g., 'Yes', 'No').                         |
|cost             |INT           |Cost or Base price of the product (monetary units).                                         |
|product_line     |VARCHAR(50)   |Specifies the product line or series to which the product belongs to (e.g., Road, Mountain).|
|star_date        |DATE          |Date when the product became available for sale or use.                                     |

---

## 3. **gold_fact_sales**
- **Purpose:** Stores transactional sales data for analytical purposes
- **Columns:**

|Column Name      |Data Type     |Description                                                                            |
|-----------------|--------------|---------------------------------------------------------------------------------------|
|order_number     |VARCHAR(50)   |Alphanumeric identifier representing the sales order (e.g., 'SO43698').                |
|product_key      |INT           |Surrogate key linking the order to the product dimension table.                        |
|customer_key     |INT           |Surrogate key linking the order to the customer dimension table.                       |
|order_date       |DATE          |Date when the order was placed.                                                        |
|ship_date        |DATE          |Date when the order was shipped to customer.                                           |
|due_date         |DATE          |Date when the order payment was due.                                                   |
|sales_amount     |INT           |Total Monetary value of the sale for the line item, in whole currency units (e.g., 85).|
|quantity         |INT           |Number of units of the product ordered for the line item (e.g., 1).                    |
|price            |INT           |Price per unit of the product for the line item, in whole currency units (e.g., 85).   |
