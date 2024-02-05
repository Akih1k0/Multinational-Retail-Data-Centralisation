# Multinational Retail Data Centralisation

The Multinational Retail Data Centralization project centralizes retail data from various sources into a local database. This involves extracting data from remote databases, APIs, PDFs, and cloud storage, cleaning and transforming the data, and uploading it to a local PostgreSQL database. The project is implemented in Python and consists of modules for data handling, database connection, and data cleaning.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
  - [Retrieve, Clean and Upload](#retrieve-clean-and-upload)
  - [SQL Processing](#sql-processing)
  - [SQL Information Querying](#sql-information-querying)
- [Modules](#modules)
  - [Database_utils](#database_utils)
  - [Data_extraction](#data_extraction)
  - [Data_cleaning](#data_cleaning)
  - [SQL Queries](#sql-queries)
- [What I have learnt](#what-i-have-learnt)
- [Project Structure](#project-structure)
- [License](#license)

## Installation
1. Clone the repository: `git clone [repository-url]`
2. Install all required packages shown in `imports.txt` using `pip install -r imports.txt`

## Usage
The usage can depend on the results required. It may be that the data has yet to be obtained or that certain queries are needed to obtain information from the data.
### Retrieve, Clean and Upload
Run `database_utils.py`. This will retrieve the data clean it and then upload it to the local Postgres.
### SQL Processing
To change the data types and add primary and secondary keys the necessary sql files are needed. Run `/data_type_queries` and run `/data_type_queries/table_type_test.sql` to test each data type has been successfully modified.
### SQL Information Querying
Run the files in `/sql_tasks` to obtain the information tasked.

## Modules
### Database_utils
This module provides a DatabaseConnector class responsible for connecting to a remote PostgreSQL database, reading credentials from a YAML file, initializing an SQLAlchemy database engine, listing database tables, and uploading a Pandas DataFrame to a specified table.

### Data_extraction
The DataExtractor class in this module is responsible for extracting data from various sources, including remote databases, PDFs, APIs, and S3 buckets.

### Data_cleaning
The DataCleaning class in this module includes functions for cleaning and processing user data, card data, store data, product data, orders data, and date details.

### SQL queries
The queries used consist of queries to set data types, further clean tables and add primary and foreign keys. The queries link relavent tables together using common columns such as user_uuid.This is done to setup the data to be queried to obtain the information needed and this is also done through sql queries.

## What I have learnt
I gained valuable insights into data handling, extraction, and integration within a relational database system. The project involved the creation of a data pipeline, utilizing Python to extract information from diverse sources such as databases, APIs, PDFs, and AWS cloud storage. Cleaning and transforming this data involved intricate processes, including standardizing formats, handling null values, and ensuring data consistency. The integration of primary and foreign keys, along with SQL queries, enhanced the database's structure and integrity. 

## Project Structure
- `README.md`: The main documentation file for the project.
- `/csv_files`: Folder containing CSV data files.
  - `date_details`: Contains the the date details.
  - `orders_table.csv`: Data of the orders.
  - `product.csv`: Product-related data.
  - `store_data.csv`: Contains data of the stores details.
  - `weight_change`: Holds the product data with the weight calculation changes.
- `/sql`: Folder containing all sql queries.
  - `/data_types_queries`: Queries used for cleaning and processing.
    - `dim_card_details_types.sql`: Applies the relevant data types to the dim_card_details table
    - `dim_date_time_types.sql`: Applies the relevant data types to the dim_date_time table.
    - `dim_products_types.sql`: Applies the relevant data types to the dim_card_details table.
    - `dim_store_details_types.sql`: Applies the relevant data types to the dim_products table.
    - `dim_tables_primary_keys.sql`: Sets the primary keys of all the tables.
    - `dim_users_types.sql`: Applies the relevant data types to the dim_users table.
    - `foreign_keys.sql`: Sets the foreign keys of all the tables.
    - `order_table_types.sql`: Applies the relevant data types to the orders_table.
    - `table_type_test.sql`: Tests the data types and shows the final tables.
  - `/query_results_png`: PNG files of the query results from the different tasks.
    - `task_1.png`: Query 1 results.
    - `task_2.png`: Query 2 results.
    - `task_3.png`: Query 3 results.
    - `task_4.png`: Query 4 results.
    - `task_5.png`: Query 5 results.
    - `task_6.png`: Query 6 results.
    - `task_7.png`: Query 7 results.
    - `task_8.png`: Query 8 results.
    - `task_9.png`: Query 9 results.
  - `/sql_tasks`: Queries to fulfil task requirements.
    - `Task_1.sql`: Query 1.
    - `Task_2.sql`: Query 2.
    - `Task_3.sql`: Query 3.
    - `Task_4.sql`: Query 4.
    - `Task_5.sql`: Query 5.
    - `Task_6.sql`: Query 6.
    - `Task_7.sql`: Query 7.
    - `Task_8.sql`: Query 8.
    - `Task_9.sql`: Query 9.
  - `query_results.md`: Markdown file containing the full results of the query tasks.
- `data_cleaning.py`: Script for cleaning data.
- `data_extraction.py`: Script for extracting data from various sources.
- `database_utils.py`: Utilities for database operations.
- `local_creds.yaml`: Holds the postgres database details.
- `imports.txt`: Text file containing all imported packages adn libraries.