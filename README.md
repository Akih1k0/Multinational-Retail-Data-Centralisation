# Multinational Retail Data Centralisation

The Multinational Retail Data Centralization project centralizes retail data from various sources into a local database. This involves extracting data from remote databases, APIs, PDFs, and cloud storage, cleaning and transforming the data, and uploading it to a local PostgreSQL database. The project is implemented in Python and consists of modules for data handling, database connection, and data cleaning.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Modules](#modules)
  - [1. main.py](#1-mainpy)
  - [2. database_utils.py](#2-database_utilspy)
  - [3. data_extraction.py](#3-data_extractionpy)
  - [4. data_cleaning.py](#4-data_cleaningpy)
- [Project Structure](#project-structure)
- [License](#license)

## ## Installation
1. Clone the repository: `git clone [repository-url]`
2. Install all required packages shown in `imports.txt` using `pip install -r imports.txt`

## Usage
Execute the main script to process and upload data to the local database:

- DataExtractor: This class used in `data_extraction.py` uses methods to retrieve the data, Whether thatis from an API, AWS S3, AWS RDS or a PDF.
- DataCleaning: This class used in `data_cleaning.py` uses methods to process and clean the data retrieved.
- DatabaseConnector: This class used in`database_utils.py` uses methods to upload the processed files to the local postgres database called sales_data.
- Main: To run all uploads or selected uploads edit the `main.py` file choosing what data needs to be uploaded using the function names.

## Modules

1. main.py
The main script orchestrates the data processing and upload tasks. It includes functions to upload user data, card details, store details, product details, product orders, and date details to the local database.

2. database_utils.py
This module provides a DatabaseConnector class responsible for connecting to a remote PostgreSQL database, reading credentials from a YAML file, initializing an SQLAlchemy database engine, listing database tables, and uploading a Pandas DataFrame to a specified table.

3. data_extraction.py
The DataExtractor class in this module is responsible for extracting data from various sources, including remote databases, PDFs, APIs, and S3 buckets.

4. data_cleaning.py
The DataCleaning class in this module includes functions for cleaning and processing user data, card data, store data, product data, orders data, and date details.

Methods:
clean_user_data(df): Cleans and processes user data, handling null values, date formats, country codes, and phone numbers.
clean_card_data(df): Cleans and processes card data, handling null values, and correcting invalid card numbers.
clean_store_data(df): Cleans and processes store data, handling null values, correcting continent names, and processing country codes.
convert_product_weights(df): Converts and standardizes product weights, handling different units.
clean_product_data(df): Cleans and processes product data, handling null values, converting price formats, and correcting column names.
clean_orders_table(df): Cleans and processes orders data, handling null values and filtering based on valid UUIDs.
clean_dates(df): Cleans and processes date details, handling null values and validating time periods.
null_cleaning(df): Cleans null values, drops duplicates, and sets the index.
valid_uuid(uuid_test): Tests if a given UUID is valid.

## Project Structure

- `README.md`: The main documentation file for the project.
- `/csv_files`: Folder containing CSV data files.
  - `date_details`: Contains the the date details.
  - `orders_table.csv`: Data of the orders.
  - `product.csv`: Product-related data.
  - `store_data.csv`: Contains data of the stores details.
  - `weight_change`: Holds the product data with the weight calculation changes.
- `data_cleaning.py`: Script for cleaning data.
- `data_extraction.py`: Script for extracting data from various sources.
- `database_utils.py`: Utilities for database operations.
- `db_creds_local.yaml`: Holds the postgres database details.
- `imports.txt`: Text file containing all imported packages adn libraries.
- `main.py`: Imports all the classes and has script to run all processing and uploading.

## License