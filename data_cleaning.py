# Imports.
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import re
import uuid


class DataCleaning:


# Cleans the users data
    def clean_user_data(self, df):
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

        # Get rid of Null and duplicates
        self.null_cleaning(df)

        # Set to a uniform date using datetime format
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce').dt.strftime('%Y-%m-%d')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce').dt.strftime('%Y-%m-%d')

        # Make the country code for UK GB
        df.loc[df['country'] == 'United Kingdom', 'country_code'] = 'GB'

        # Gets rid of phone numbers in invalid form using regex
        uk_regex = r"^(?:(?:\+44\s?\(0\)\s?\d{2,4}|\(?\d{2,5}\)?)\s?\d{3,4}\s?\d{3,4}$|\d{10,11}|\+44\s?\d{2,5}\s?\d{3,4}\s?\d{3,4})$"
        de_regex = r"(\(?([\d \-\)\–\+\/\(]+){6,}\)?([ .\-–\/]?)([\d]+))"
        us_regex = r"\(?\d{3}\)?-? *\d{3}-? *-?\d{4}"

        df.loc[(df['country_code'] == 'GB') & (~df['phone_number'].astype(str).str.match(uk_regex)), 'phone_number',] = np.nan
        df.loc[(df['country_code'] == 'DE') & (~df['phone_number'].astype(str).str.match(de_regex)), 'phone_number',] = np.nan
        df.loc[(df['country_code'] == 'US') & (~df['phone_number'].astype(str).str.match(us_regex)), 'phone_number',] = np.nan
        
        return df

# Cleans the card data
    def clean_card_data(self, df):
        """Cleans and processes user data.

        Args:
            df (pandas.DataFrame): The user data.

        Returns:
            pandas.DataFrame: The cleaned and processed user data.

        """

        # Get rid of Null and duplicates
        self.null_cleaning(df)

        # Set to a uniform date using datetime format
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce').dt.strftime('%Y-%m-%d')

        # Corrects invalid numbers
        df['card_number'] = df['card_number'].apply(str)
        df['card_number'] = df['card_number'].str.replace('\W', '', regex=True)

        return df

# Cleans store data
    def clean_store_data(self, df):
        """Cleans and processes card data.

        Args:
            df (pandas.DataFrame): The card data.

        Returns:
            pandas.DataFrame: The cleaned and processed card data.

        """

        # Makes Null strings NaN
        df.replace('NULL', np.nan, inplace=True)

        # Removes exta latitude column
        df.drop(columns='lat', inplace=True)

        # Corrects continent names and processes invalid country codes
        df.loc[df['continent'] == 'eeEurope', 'continent'] = 'Europe'
        df.loc[df['continent'] == 'eeAmerica', 'continent'] = 'America'

        df.loc[~df['country_code'].isin(['GB','DE', 'US']), 'country_code'] = np.nan

        # Set to a uniform date using datetime format
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce').dt.strftime('%Y-%m-%d')

        # Staff numbers processing
        df['staff_numbers'] = df['staff_numbers'].str.replace('[^0-9]', '')

        # Replace N/A with NaN
        df.dropna(inplace=True)
        df.replace('N/A', np.nan, inplace=True)
        df.set_index(['index'], inplace=True)
        
        return df
    
# Unit conversion of the weight column
    def convert_product_weights(self, df):
        """Converts and standardizes product weights.

        Args:
            df (pandas.DataFrame): The product data.

        Returns:
            pandas.DataFrame: The product data with standardized weights.

        """

        # Converts values into string format
        df['weight'] = df['weight'].apply(str)

        # Copies weight column for referencing later on
        df['weight_1'] = df['weight']

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
        df["weight"] = df["weight_1"].apply(convert_to_kg)

        # Creates units column
        units = re.compile(r'(\d+)([gkmlKGML]+)')
        df['units'] = df['weight_1'].str.extract(units).iloc[:, 1]

        # Converts all values to kg form
        df['weight'] = df.apply(lambda x: x['weight']/1000 if x['units']== 'g' or x['units']== 'ml' else x['weight'], axis=1)

        # Removes the columns used for referencing
        df.drop(columns=['units', 'weight_1'], inplace=True)

        return df
    
# Cleans Product data
    def clean_product_data(self, df):
        """Cleans and processes product data.

        Args:
            df (pandas.DataFrame): The product data.

        Returns:
            pandas.DataFrame: The cleaned and processed product data.

        """
        
        # Removing special characters from start of price
        df['product_price'] = pd.to_numeric(df['product_price'].str.slice(1), errors='coerce').round(2)

        # Set to a uniform date using datetime format
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce').dt.date

        # Make the avalability column boolean data of True or False and fix spelling mistake iof available
        df['removed'] = np.where(df['removed'] == 'Still_avaliable', True, False)

        # UUIDs processing
        uuid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
        df = df[df["uuid"].apply(lambda x: bool(re.match(uuid_pattern, str(x))))]

        # Giving columns appropriate names
        df_copy = df.copy()
        df = df_copy.rename(columns={'Unnamed: 0': 'index',
                           'weight': 'weight_kg',
                           'EAN': 'ean',
                           'product_price': 'product_price_gbp',
                           'removed': 'available',
                           'uuid': 'user_uuid'}
                           )

        # Sets index
        df.set_index(['index'], inplace=True)

        return df
    
    def clean_orders_table(self, df):
        """Cleans and processes orders data.

        Args:
            df (pandas.DataFrame): The orders data.

        Returns:
            pandas.DataFrame: The cleaned and processed orders data.

        """

        # Processing columns to keep whats relevant
        df.drop(columns=['first_name', 'last_name', '1'], inplace=True, errors='ignore')
        df.set_index(['index'], inplace=True)
        df.rename(columns={'level_0': 'order_id'}, inplace=True)

        # UUID testing
        df = df[df['user_uuid'].apply(lambda x: valid_uuid(x))]
        df = df[df['date_uuid'].apply(lambda x: valid_uuid(x))]

        # Null cleaning
        self.null_cleaning(df)

        return df
    
# Cleans date details
    def clean_dates(self, df):
        """Cleans and processes date details.

        Args:
            df (pandas.DataFrame): The date details data.

        Returns:
            pandas.DataFrame: The cleaned and processed date details data.

        """

        # Removes null values in the specified columns
        self.null_cleaning(df)

        # Validating time periods
        valid_period = ["Late_Hours", "Morning", "Midday", "Evening"]
        df = df[df['time_period'].isin(valid_period)]

         # Changes values into numeric format
        df.loc[:, 'day'] = pd.to_numeric(df['day'], errors='coerce')
        df.loc[:, 'month'] = pd.to_numeric(df['month'], errors='coerce')
        df.loc[:, 'year'] = pd.to_numeric(df['year'], errors='coerce')
        
        return df

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