/ *
Create Database and Schemas
=========================1
Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze' , 'silver', and 'gold'.
  */

-- Drop and recreate the 'DataWarehouse' database in MySQL

DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE DataWarehouse;

-- Use the new database
USE DataWarehouse;

-- Create tables with schema-like prefixes
CREATE TABLE bronze_example (
    id INT PRIMARY KEY,
    data VARCHAR(255)
);

CREATE TABLE silver_example (
    id INT PRIMARY KEY,
    data VARCHAR(255)
);

CREATE TABLE gold_example (
    id INT PRIMARY KEY,
    data VARCHAR(255)
);
