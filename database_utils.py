import psycopg2
import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnector:
    """A class for connecting to a remote PostgreSQL database and performing operations.

    Attributes:
        None

    Methods:
        read_db_creds(name):
            Reads and returns the database credentials from a YAML file.

        init_db_engine(creds):
            Initializes and returns an SQLAlchemy database engine using provided credentials.

        list_db_tables(engine):
            Retrieves and returns a list of table names in the connected database.

        upload_to_db(df, table_name, engine):
            Uploads a pandas DataFrame to a specified table in the connected database.

    """

    def read_db_creds(self, name):
        """Reads the database credentials from a YAML file.

        Args:
            name (str): The file path of the YAML file containing database credentials.

        Returns:
            dict: A dictionary containing the database credentials.

        """
        with open(name, 'r') as stream:
            return yaml.safe_load(stream)

    def init_db_engine(self, creds):
        """Initializes an SQLAlchemy database engine using provided credentials.

        Args:
            creds (dict): A dictionary containing the database credentials.

        Returns:
            sqlalchemy.engine.Engine: An SQLAlchemy database engine.

        """
        engine = create_engine(
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@"
            f"{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
        return engine

    def list_db_tables(self, engine):
        """Retrieves a list of table names in the connected database.

        Args:
            engine (sqlalchemy.engine.Engine): An SQLAlchemy database engine.

        Returns:
            list: A list of table names.

        """
        inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name, engine):
        """Uploads a pandas DataFrame to a specified table in the connected database.

        Args:
            df (pandas.DataFrame): The DataFrame to be uploaded.
            table_name (str): The name of the table in the database.
            engine (sqlalchemy.engine.Engine): An SQLAlchemy database engine.

        Returns:
            pandas.DataFrame: The input DataFrame.

        """
        df.to_sql(table_name, engine, index=False, if_exists='replace')
        return df