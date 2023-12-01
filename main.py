#%%
import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Classes and local engine
de = DataExtractor()
db = DatabaseConnector()
dc = DataCleaning()
local_engine = db.init_db_engine(db.read_db_creds('db_creds_local.yaml'))

def upload_user():
    """Uploads user data to a local database after cleaning.

    Retrieves data from the remote database, cleans it using DataCleaning class,
    and uploads it to the local database using DatabaseConnector class.
    """
    # Retrieves data.
    engine = db.init_db_engine(db.read_db_creds("db_creds.yaml"))
    engine.connect()
    tables = db.list_db_tables(engine)
    df = de.read_rds_table(engine, tables[1])

    # Clean data.
    df_clean = dc.clean_user_data(df)

    # Upload data.
    db.upload_to_db(df_clean, 'dim_users', local_engine)

def upload_card_details():
    """Uploads card details data to a local database after cleaning.

    Retrieves data from a PDF, cleans it using DataCleaning class,
    and uploads it to the local database using DatabaseConnector class.
    """
    # Get data from PDF.
    df = de.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    # Clean data.
    df_clean = dc.clean_card_data(df)

    # Upload data.
    db.upload_to_db(df_clean, 'dim_card_details', local_engine)

def upload_store_details():
    """Uploads store details data to a local database after cleaning.

    Retrieves data from an API, cleans it using DataCleaning class,
    and uploads it to the local database using DatabaseConnector class.
    """
    # Get data from API.
    store_info = de.list_number_of_stores()
    number_of_stores = store_info.get('number_of_stores', 451)
    df = de.retrieve_stores_data(number_of_stores=number_of_stores)

    # Clean data.
    df_clean = dc.clean_store_data(df)

    # Upload data.
    db.upload_to_db(df_clean, 'dim_store_details', local_engine)

def upload_product_details():
    """Uploads product details data to a local database after cleaning.

    Retrieves data from S3, converts and cleans it using DataCleaning class,
    and uploads it to the local database using DatabaseConnector class.
    """
    # Get data from S3.
    df = de.extract_from_s3('s3://data-handling-public/products.csv')

    # Clean data.
    df_weight = dc.convert_product_weights(df)
    df_clean = dc.clean_product_data(df_weight)

    # Upload data.
    engine = db.init_db_engine(db.read_db_creds('db_creds_local.yaml'))
    db.upload_to_db(df_clean, 'dim_products', engine)

def product_orders():
    """Uploads product orders data to a local database after cleaning.

    Retrieves data from the remote database, cleans it using DataCleaning class,
    and uploads it to the local database using DatabaseConnector class.
    """
    # Retrieve orders table.
    engine = db.init_db_engine(db.read_db_creds("db_creds.yaml"))
    tables = db.list_db_tables(engine)
    df = de.read_rds_table(engine, tables[2])

    # Cleaning order table.
    df_clean = dc.clean_orders_table(df)

    # Upload data.
    db.upload_to_db(df_clean, 'orders_table', local_engine)

def date_details():
    """Uploads date details data to a local database after cleaning.

    Retrieves data from S3, cleans it using DataCleaning class,
    and uploads it to the local database using DatabaseConnector class.
    """
    # Retrieve data.
    df = de.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

    # Cleans data.
    df_clean = dc.clean_dates(df)

    # Uploads data.
    db.upload_to_db(df_clean, 'dim_date_times', local_engine)


if __name__ == '__main__':
    """
    Main script to process and upload data to the local database.
    """
    # Upload user data
    upload_user()

    # Upload card details data
    upload_card_details()

    # Upload store details data
    upload_store_details()

    # Upload product details data
    upload_product_details()

    # Upload product orders data
    product_orders()

    # Upload date details data
    date_details()
# %%
