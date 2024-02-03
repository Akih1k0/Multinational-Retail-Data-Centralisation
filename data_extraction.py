# Imports
from database_utils import DatabaseConnector
import boto3
import pandas as pd
import requests
import tabula
from arguments import HEADERS, NO_STORES_URL, STORE_DETAIL_URL, S3_URL_PATH


class DataExtractor:
    """A class for extracting data from various sources.

    Attributes:
        HEADERS (dict): A dictionary containing API headers.
        NO_STORES_URL (str): The URL for retrieving the number of stores API data.
        STORE_DETAIL_URL (str): The URL for retrieving store details API data.
        S3_URL_PATH (str): The path to the S3 data file.

    Methods:
        read_rds_table(engine, table_name):
            Reads and returns data from a specified RDS table.

        retrieve_pdf_data(link):
            Retrieves data from a PDF file specified by the link.

        list_number_of_stores(url=NO_STORES_URL, headers=HEADERS):
            Retrieves and returns the number of stores from an API.

        retrieve_stores_data(number_of_stores=451, main_url=STORE_DETAIL_URL, headers=HEADERS):
            Retrieves and returns store data from an API.

        extract_from_s3(url):
            Retrieves and returns data from an S3 bucket based on the provided URL.

    """

    # DatabaseConnector class
    db = DatabaseConnector()

    # Engine and Table
    engine = db.init_db_engine(db.read_db_creds('db_creds.yaml'))
    rds_tables = db.list_db_tables(engine)

    def read_rds_table(self, table_name, engine=engine):
        """Reads and returns data from a specified RDS table.

        Args:
            engine (sqlalchemy.engine.Engine): An SQLAlchemy database engine.
            table_name (str): The name of the table to read.

        Returns:
            pandas.DataFrame: The data from the specified RDS table.

        """
        with engine.connect():
            return pd.read_sql(table_name, engine)

    def retrieve_pdf_data(self, link='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'):
        """Retrieves data from a PDF file specified by the link.

        Args:
            link (str): The link to the PDF file.

        Returns:
            pandas.DataFrame: The extracted data from the PDF.

        """
        dfs = tabula.read_pdf(link, pages='all')
        return pd.concat(dfs)

    def list_number_of_stores(self, url=NO_STORES_URL, headers=HEADERS):
        """Retrieves and returns the number of stores from an API.

        Args:
            url (str): The URL of the API.
            headers (dict): The headers for the API request.

        Returns:
            dict: The JSON response containing the number of stores.

        """
        stores = requests.get(url, headers=headers)
        number_of_stores = stores.json()
        return number_of_stores['number_stores']

    def retrieve_stores_data(self, main_url=STORE_DETAIL_URL, headers=HEADERS):
        """Retrieves and returns store data from an API.

        Args:
            number_of_stores (int): The number of stores to retrieve data for.
            main_url (str): The main URL for retrieving store details.
            headers (dict): The headers for the API request.

        Returns:
            pandas.DataFrame: The extracted store data.

        """
        store_list = []
        number_of_stores = self.list_number_of_stores()
        for store_number in range(0, number_of_stores):
            url = f'{main_url}/'+str(store_number)
            response = requests.get(url, headers=headers).json()
            store_list.append(response)

        return pd.json_normalize(store_list)

    def extract_from_s3(self, url):
        """Retrieves and returns data from an S3 bucket based on the provided URL.

        Args:
            url (str): The S3 URL specifying the bucket and path.

        Returns:
            pandas.DataFrame: The extracted data from the S3 bucket.

        Raises:
            ValueError: If the file type is unsupported (not CSV or JSON).

        """
        bucket = url.split('/')[2].split('.')[0]
        path = '/'.join(url.split('/')[3:])
        file_type = path.split('.')[-1]
        s3 = boto3.client('s3')

        obj = s3.get_object(Bucket=bucket, Key=path)
        if file_type.lower() == 'json':
            df = pd.read_json(obj['Body'])
        elif file_type.lower() == 'csv':
            df = pd.read_csv(obj['Body'])
        else:
            raise ValueError('Unsupported file type. File is not a CSV or JSON file')

        return df
    