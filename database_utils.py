#%%
# Imports
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

    def read_db_creds(self, file_name):
        """Reads the database credentials from a YAML file.

        Args:
            name (str): The file path of the YAML file containing database credentials.

        Returns:
            dict: A dictionary containing the database credentials.

        """

        with open(file_name, 'r') as stream:
            return yaml.safe_load(stream)

    def init_db_engine(self, creds):
        """Initializes an SQLAlchemy database engine using provided credentials.

        Args:
            creds (dict): A dictionary containing the database credentials.

        Returns:
            sqlalchemy.engine.Engine: An SQLAlchemy database engine.

        """

        engine = create_engine(
            f"postgresql://{creds['USER']}:{creds['PASSWORD']}@"
            f"{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}"
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
    
if __name__ == '__main__':

    from data_cleaning import DataCleaning
    # Classes and engine
    db = DatabaseConnector()
    dc = DataCleaning()
    local_engine = db.init_db_engine(db.read_db_creds('db_creds_local.yaml'))

    # Clean data
    clean_users = dc.clean_user_data()
    clean_card_details = dc.clean_card_data()
    clean_store_details = dc.clean_store_data()
    clean_products = dc.clean_product_data()
    clean_orders = dc.clean_orders_table()
    clean_dates = dc.clean_dates()

    # Upload data
    clean_dict = {
        'dim_users': clean_users,
        'dim_card_details': clean_card_details,
        'dim_store_details': clean_store_details,
        'dim_products': clean_products,
        'orders_table': clean_orders,
        'dim_date_times': clean_dates
    }

    for name, clean_data in clean_dict.items():
        db.upload_to_db(clean_data, name, local_engine)
# %%
