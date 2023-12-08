# Imports.
from data_extraction import DataExtractor
import pandas as pd
import numpy as np
import re
import uuid


class DataCleaning:

    def __init__(self):
        self.de = DataExtractor()

# Cleans the users data
    def clean_user_data(self):
        """A class for cleaning and transforming data.

    Attributes:
        uuid_pattern (str): A regular expression pattern for validating UUIDs.

    Methods:
        clean_user_data(df):
            Cleans and processes user data, handling null values, date formats, country codes, and phone numbers.

        clean_card_data(df):
            Cleans and processes card data, handling null values, and correcting invalid card numbers.

        clean_store_data(df):
            Cleans and processes store data, handling null values, correcting continent names, and processing country codes.

        convert_product_weights(df):
            Converts and standardizes product weights, handling different units.

        clean_product_data(df):
            Cleans and processes product data, handling null values, converting price formats, and correcting column names.

        clean_orders_table(df):
            Cleans and processes orders data, handling null values and filtering based on valid UUIDs.

        clean_dates(df):
            Cleans and processes date details, handling null values and validating time periods.

        null_cleaning(df):
            Cleans null values, drops duplicates, and sets the index.

        valid_uuid(uuid_test):
            Tests if a given UUID is valid.

    """

        # Read data
        user_df = self.de.read_rds_table('legacy_users')
        
        # Get rid of Null and duplicates
        self.null_cleaning(user_df)

        # Set to a uniform date using datetime format
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'], errors='coerce').dt.strftime('%Y-%m-%d')
        user_df['join_date'] = pd.to_datetime(user_df['join_date'], errors='coerce').dt.strftime('%Y-%m-%d')

        # Make the country code for UK GB
        user_df.loc[user_df['country'] == 'United Kingdom', 'country_code'] = 'GB'

        # Gets rid of phone numbers in invalid form using regex
        uk_regex = r"^(?:(?:\+44\s?\(0\)\s?\d{2,4}|\(?\d{2,5}\)?)\s?\d{3,4}\s?\d{3,4}$|\d{10,11}|\+44\s?\d{2,5}\s?\d{3,4}\s?\d{3,4})$"
        de_regex = r"(\(?([\d \-\)\–\+\/\(]+){6,}\)?([ .\-–\/]?)([\d]+))"
        us_regex = r"\(?\d{3}\)?-? *\d{3}-? *-?\d{4}"

        user_df.loc[(user_df['country_code'] == 'GB') & (~user_df['phone_number'].astype(str).str.match(uk_regex)), 'phone_number',] = np.nan
        user_df.loc[(user_df['country_code'] == 'DE') & (~user_df['phone_number'].astype(str).str.match(de_regex)), 'phone_number',] = np.nan
        user_df.loc[(user_df['country_code'] == 'US') & (~user_df['phone_number'].astype(str).str.match(us_regex)), 'phone_number',] = np.nan

        user_df = user_df[user_df['user_uuid'].apply(lambda x: valid_uuid(x))]
        
        return user_df

# Cleans the card data
    def clean_card_data(self):
        """Cleans and processes user data.

        Args:
            df (pandas.DataFrame): The user data.

        Returns:
            pandas.DataFrame: The cleaned and processed user data.

        """

        # Read data
        card_df = self.de.retrieve_pdf_data()

        # Get rid of Null and duplicates
        self.null_cleaning(card_df)

        # Set to a uniform date using datetime format
        card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], errors='coerce').dt.strftime('%Y-%m-%d')

        # Corrects invalid numbers
        card_df['card_number'] = card_df['card_number'].apply(str)
        card_df['card_number'] = card_df['card_number'].str.replace('\W', '', regex=True)

        return card_df

# Cleans store data
    def clean_store_data(self):
        """Cleans and processes card data.

        Args:
            df (pandas.DataFrame): The card data.

        Returns:
            pandas.DataFrame: The cleaned and processed card data.

        """
        
        # Read data
        store_df = self.de.retrieve_stores_data()

        # Removes exta latitude column
        store_df.drop(columns='lat', inplace=True)

        # Corrects continent names and processes invalid country codes
        store_df.loc[store_df['continent'] == 'eeEurope', 'continent'] = 'Europe'
        store_df.loc[store_df['continent'] == 'eeAmerica', 'continent'] = 'America'

        # Set to a uniform date using datetime format
        store_df['opening_date'] = pd.to_datetime(store_df['opening_date'], errors='coerce').dt.strftime('%Y-%m-%d')

        # Staff numbers processing
        store_df['staff_numbers'] = pd.to_numeric(store_df['staff_numbers'].str.replace(r'\D', '', regex=True))

        # Replace N/A with NaN
        store_df.replace('N/A', np.nan, inplace=True)

        # Makes sure all country codes are valid
        valid_country_codes = ['GB', 'US', 'DE']
        store_df = store_df[store_df['country_code'].isin(valid_country_codes)]
        
        return store_df
    
# Unit conversion of the weight column
    def convert_product_weights(self):
        """Converts and standardizes product weights.

        Args:
            df (pandas.DataFrame): The product data.

        Returns:
            pandas.DataFrame: The product data with standardized weights.

        """

        # Reads data
        product_df = self.de.extract_from_s3('s3://data-handling-public/products.csv')

        # Converts values into string format
        product_df['weight'] = product_df['weight'].apply(str)

        # Copies weight column for referencing later on
        product_df['weight_1'] = product_df['weight']

        # Converts multiplication
        def convert_to_kg(weight_str):
            if "x" in weight_str:
                parts = weight_str.split("x")
                try:
                    quantity = float(re.findall(r"(\d+(\.\d+)?)", parts[0])[0][0])
                    unit_weight = float(re.findall(r"(\d+(\.\d+)?)", parts[1])[0][0])
                    total_weight = quantity * unit_weight
                    return total_weight
                except (IndexError, ValueError):
                    return None
            else:
                # Extract numeric weight from string
                numeric = re.findall(r"(\d+(\.\d+)?)", weight_str)
                if numeric:
                    total_weight = float(numeric[0][0])
                    return total_weight
                else:
                    return None
                
        # Applies the conditions above to complete the multiplication and return the float value
        product_df["weight"] = product_df["weight_1"].apply(convert_to_kg)

        # Creates units column
        units = re.compile(r'(\d+)([gkmlKGML]+)')
        product_df['units'] = product_df['weight_1'].str.extract(units).iloc[:, 1]

        # Converts all values to kg form
        product_df['weight'] = product_df.apply(lambda x: x['weight']/1000 if x['units']== 'g' or x['units']== 'ml' else x['weight'], axis=1)

        # Removes the columns used for referencing
        product_df.drop(columns=['units', 'weight_1'], inplace=True)

        return product_df
    
# Cleans product data
    def clean_product_data(self):
        """Cleans and processes product data.

        Args:
            df (pandas.DataFrame): The product data.

        Returns:
            pandas.DataFrame: The cleaned and processed product data.

        """
        
        # Gets data
        product_df = self.convert_product_weights()

        # Removing special characters from start of price
        product_df['product_price'] = pd.to_numeric(product_df['product_price'].str.slice(1), errors='coerce').round(2)

        # Set to a uniform date using datetime format
        product_df['date_added'] = pd.to_datetime(product_df['date_added'], errors='coerce').dt.date

        # Make the avalability column boolean data of True or False and fix spelling mistake iof available
        product_df['removed'] = np.where(product_df['removed'] == 'Still_avaliable', True, False)

        # UUIDs processing
        uuid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
        product_df = product_df[product_df["uuid"].apply(lambda x: bool(re.match(uuid_pattern, str(x))))]

        # Giving columns appropriate names
        product_df_copy = product_df.copy()
        product_df = product_df_copy.rename(columns={'Unnamed: 0': 'index',
                           'weight': 'weight_kg',
                           'EAN': 'ean',
                           'product_price': 'product_price_gbp',
                           'removed': 'still_available'}
                           )

        # Sets index
        product_df.set_index(['index'], inplace=True)

        return product_df
    
# Cleans orders data
    def clean_orders_table(self):
        """Cleans and processes orders data.

        Args:
            df (pandas.DataFrame): The orders data.

        Returns:
            pandas.DataFrame: The cleaned and processed orders data.

        """

        # Reads data
        order_df = self.de.read_rds_table('orders_table')
        # Processing columns to keep whats relevant
        order_df.drop(columns=['first_name', 'last_name', '1'], inplace=True, errors='ignore')

        # Null cleaning
        order_df.rename(columns={'level_0': 'order_id'}, inplace=True)

        # UUID testing
        order_df = order_df[order_df['user_uuid'].apply(lambda x: valid_uuid(x))]

        # Index
        order_df.set_index(['index'], inplace=True)

        return order_df
    
# Cleans date details
    def clean_dates(self):
        """Cleans and processes date details.

        Args:
            df (pandas.DataFrame): The date details data.

        Returns:
            pandas.DataFrame: The cleaned and processed date details data.

        """

        # Reads data
        data_df = self.de.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

        # Removes null values in the specified columns
        self.null_cleaning(data_df)

        # Validating time periods
        valid_period = ["Late_Hours", "Morning", "Midday", "Evening"]
        data_df = data_df[data_df['time_period'].isin(valid_period)]

         # Changes values into numeric format
        data_df.loc[:, 'day'] = pd.to_numeric(data_df['day'], errors='coerce')
        data_df.loc[:, 'month'] = pd.to_numeric(data_df['month'], errors='coerce')
        data_df.loc[:, 'year'] = pd.to_numeric(data_df['year'], errors='coerce')
        
        return data_df

# Funct to clean all Null values
    def null_cleaning(self, df):
        """Cleans null values, duplicates, and sets index.

    Args:
        df (pd.DataFrame): The DataFrame to be cleaned.

    Returns:
        None: Modifies the input DataFrame in place.
    """
        df.replace('NULL', np.nan, inplace=True)
        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)

        for i in df.columns:
            if i == 'index':
                df.set_index(i, inplace=True)

# Funct testing UUIDs
def valid_uuid(uuid_test):
    """Cleans null values, duplicates, and sets index.

    Args:
        df (pd.DataFrame): The DataFrame to be cleaned.

    Returns:
        None: Modifies the input DataFrame in place.
    """

    try:
        uuid_obj = uuid.UUID(uuid_test)
    except ValueError:
        return False
    return str(uuid_obj) == uuid_test