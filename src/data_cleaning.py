import pandas as pd
import pycountry
import re
from validate_email import validate_email

class DataCleaning:
    """
    A class for cleaning data from various sources.
    """

    def __init__(self):
        pass  # The constructor method is empty, as no initialization is needed

    # Task 3 step 6
    def clean_user_data(self, df):  # Define a method to clean the user data DataFrame (Task 3, Step 6)
        """
        Clean the user data DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame containing user data to be cleaned.

        Returns:
            pandas.DataFrame: The cleaned user data DataFrame.
        """

        # Validating first and last name, and converting column to string data type.

        df['first_name'] = df['first_name'].apply(lambda x: x if re.match(r'^[a-zA-Z\s-]+$', str(x)) else None)
        df['first_name'] = df['first_name'].astype('string')
        df = df.dropna(subset=['first_name'])

        df['last_name'] = df['last_name'].apply(lambda x : x if re.match(r'^[a-zA-Z\s-]+$', str(x)) else None)
        df['last_name'] = df['last_name'].astype('string')
        df = df.dropna(subset=['last_name'])

        # Converting date_of_birth column to datetime format and removing rows with invalid dates
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        df = df.dropna(subset=['date_of_birth'])

        # Converting company name column to string data type
        df['company'] = df['company'].astype('string')
        
        # Validating email_address and converting to string type
        df['email_address'] = df['email_address'].apply(lambda x: x if validate_email(x) else None)
        df = df.dropna(subset=['email_address'])
        df['email_address'] = df['email_address'].astype('string')

        # Converting address column name to string data type
        df['address'] = df['address'].astype('string')

        # Validating country name entry and converting column to string data type
        df['country'] = df['country'].apply(lambda x: x if pycountry.countries.lookup(x)
                                            or pycountry.countries.get(alpha_2=x.country)
                                            or pycountry.countries.get(alpha_3=x.country) else None)
        df['country'] = df['country'].astype('string')

        # Validating country code entires and converting column to string data type
        df['country_code'] = df.apply(lambda x: x.country_code if pycountry.countries.get(alpha_2=x.country_code) else None, axis='columns')
        df = df.dropna(subset=['country_code'])
        df['country_code'] = df['country'].astype('string')
        
        # Converting phone_number column to string type
        df['phone_number'] = df['phone_number'].astype('string')

        # Converting join_date  column to datetime format and removing rows with invalid dates
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        df = df.dropna(subset=['join_date'])

        # Converting user_uuid to string data type
        df['user_uuid'] = df['user_uuid'].astype('string')

        return df  # Return the cleaned user data DataFrame
    
    # Task 4 Step 3
    def clean_card_data(self, df):
        """
        Clean the card data DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame containing user data to be cleaned.

        Returns:
            pandas.DataFrame: The cleaned user data DataFrame.
        """

        # Converting card_number column to string type
        df['card_number'] = df['card_number'].astype('string')
 
        # Removing rows with invalid date format, and converting column to string type
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df.dropna(subset=['expiry_date'], inplace = True)
        df['expiry_date'] = df['expiry_date'].astype('string')

        # Converting card_provider column to string type
        df['card_provider'] = df['card_provider'].astype('string')

        # Converting date_payment_confirmed column to datetime format and removing rows with invalid dates
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df.dropna(subset=['date_payment_confirmed'], inplace = True)

        return df
    

    def clean_store_data(self, df):
        """
        Clean the store data DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame containing store data to be cleaned.

        Returns:
            pandas.DataFrame: The cleaned store data DataFrame.
        """
        # Converting address data type to string
        df['address'] = df['address'].astype('string')
 
        # # Converting the longitude column type to num
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        # Dropping the lat column as it contains many null values.
        df.drop('lat', axis='columns', inplace=True)

        # Converting locality column type to string
        df['locality'] = df['locality'].astype('string')

        # # Removing invalid store_code entries
        df.dropna(subset=['store_code'])
        df['store_code'] = df['store_code'].astype('string')

        # Converting the staff_number column type to numbers, and removing invalid entries
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        df.dropna(subset=['staff_numbers'], inplace = True)
        df['staff_numbers'] = df['staff_numbers'].astype(int)

        # # Converting opening_date column to datetime format and removing rows with invalid dates
        df['opening_date'] = pd.to_datetime(df['opening_date'], format='%Y-%m-%d', errors='coerce')
        df = df.dropna(subset=['opening_date'])

        # Converting store type column to string
        df['store_type'] = df['store_type'].astype('string')

        # converting latitude column to float32 type
        df['latitude'] = df['latitude'].astype('float32')

        # # Converting country code column to string
        df['country_code'] = df['country_code'].astype('string')

        # # Converting continent column type to string, and correcting continent miss spellings.
        df['continent'] = df['continent'].astype('string')
        df['continent'] = df['continent'].str.replace('eeEurope', 'Europe').str.replace('eeAmerica', 'America')

        return df


    def parse_weight(self, weight_str):
        """"
        Parse a weight string and return the numerical value and unit.
        If the weight string contains multiple units (e.g., "2 x 400g"), split it into separate entries.
        """
        # Define a dictionary to map the weight units to their conversion factors
        unit_mapping = {
            'kg': 1.0,
            'g': 0.001,
            'mg': 0.000001,
            'ml': 0.001,  # Assuming 1 ml = 1 g for liquids
            'l': 1.0,
            'oz': 0.028349523125,  # 1 oz = 0.028349523125 kg
        }

        if 'x' in weight_str: # Check if the weight string contains an 'x' character, indicating values such as ( "2 x 400g")
            # Split the weight string on the 'x' character, first part is the multiplier, second part is unit string
            multiplier, unit_str = weight_str.split('x')

            # Converting the multiplier string to an integer and remove any leading/trailing whitespace
            multiplier = int(multiplier.strip())

            # The regex pattern matches a numerical value (with or without a decimal point)
            # followed by one or more whitespace characters and then one or more alphabetic characters
            value, unit = re.match(r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)', unit_str.strip()).groups()

            # Get the conversion factor for the unit from the 'unit_mapping' dictionary
            unit_converted = unit_mapping.get(unit.lower())

            # Convert the value string to a float
            value = float(value)

            # Calculate the weight in kilograms by multiplying the value, multiplier, and conversion factor
            new_weight_str = value * multiplier * unit_converted

            # Return the calculated weight in kilograms
            return new_weight_str
        else:
            # The regex pattern matches a numerical value (with or without a decimal point)
            # followed by one or more whitespace characters and then one or more alphabetic characters
            match = re.match(r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)', weight_str)

            # Unpack the matched groups into 'value' and 'unit' variables
            value, unit = match.groups()

            # Convert the 'value' string to a float
            value = float(value)

            # Get the conversion factor for the 'unit' from the 'unit_mapping' dictionary
            unit_converted = unit_mapping.get(unit.lower())

            # Calculate the weight in kilograms by multiplying the value with the conversion factor
            new_weight_str = value * unit_converted

            return new_weight_str

 
    def convert_product_weights(self, df):
        """
        Convert the product weights to decimal values representing their weight in kilograms (kg).

        Args:
            products_df (pandas.DataFrame): The DataFrame containing the product data.

        Returns:
            pandas.DataFrame: The DataFrame with the 'weight' column converted to kilograms.
        """
  
        # Checking rows for invalid entries and removing them
        df['weight'] = df['weight'].apply(lambda x: x if re.match(r'^[0-9gGkKmMlLxX\. ]+$', str(x)) else None)
        df.dropna(subset=['weight'], inplace = True)

        # Standardising weight format, converting to kg, and changing column name to weight(kg)
        df['weight(kg)'] = df['weight'].apply(self.parse_weight)
        df.drop('weight', axis='columns', inplace=True)

        return df
    
    
    def clean_products_data(self, df):
        """
        Clean the store data DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame containing store data to be cleaned.

        Returns:
            pandas.DataFrame: The cleaned store data DataFrame.
        """

        # Cleaning the weight column by removing in valid entries, converting it to float format, and standardising units to kg
        self.convert_product_weights(df)

        # Converting country code column to string
        df['product_name'] = df['product_name'].astype('string')

        # Converting product_price column to float32 type 
        df['product_price(£)'] = df['product_price'].str.replace('£', '').astype('float32')
        df.drop('product_price', axis='columns', inplace=True)

        # Converting category column to string type
        df['category'] = df['category'].astype('string')
        
        # Converting EAN column to string type
        df['EAN'] = df['EAN'].astype('string')

        # Converting date_added column to datetime format and removing invalid entries
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        df.dropna(subset=['date_added'], inplace = True)

        # Converting uuid column to string type
        df['uuid'] = df['uuid'].astype('string')

        # Converting 'removed' column to string type
        df['removed'] = df['removed'].astype('string')

        # Converting product_code column to string type
        df['product_code'] = df['product_code'].astype('string')

        return df
    

    def clean_orders_data(self, df):
        """
        Clean the customer orders data DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame containing store data to be cleaned.

        Returns:
            pandas.DataFrame: The cleaned orders data DataFrame.
        """

        # Dropping the first_name, last_name and '1' column
        df.drop(['first_name', 'last_name', '1'], axis = 'columns', inplace = True)

        # Converting date_uuid column to string type
        df['date_uuid'] = df['date_uuid'].astype('string')

        # Converting user_uuid column to string type
        df['user_uuid'] = df['user_uuid'].astype('string')

        # Converting card_number column to string type
        df['card_number'] = df['card_number'].astype('string')

        # Converting store_code column to string type
        df['store_code'] = df['store_code'].astype('string')

        # Converting product_code column to string type
        df['product_code'] = df['product_code'].astype('string')
        
        return df  # Return the cleaned orders data DataFrame   

    def clean_date_events_data(self, df):
        """
        Clean the date events data DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame containing date events data to be cleaned.

        Returns:
            pandas.DataFrame: The cleaned date events data DataFrame.
        """
        # Correcting timestamp formatting, as they all begin with 1900-01-01,
        # So we will format it using the years, month, and day from the other columns an the Hours, minutes, seconds
        # from the timestamp column 


        # Removing invalid date entries 
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%H:%M:%S', errors='coerce')
        df = df.dropna(subset=['timestamp'])
        
        # Extracting time part from the timestamp
        df['time'] = df['timestamp'].dt.time

        # Replacing the timestamp column with the correct year, month, day, and time, formatted appropriately
        df['timestamp'] = pd.to_datetime(df[['year', 'month', 'day']].astype(str).agg('-'.join, axis=1) + ' ' + df['time'].astype(str))

        # Converting month column to integer data type
        df['month'] = pd.to_numeric(df['month'])

        # Converting year column to integer data type
        df['year'] = pd.to_numeric(df['year'])

        # Converting day column to integer type
        df['day'] = pd.to_numeric(df['day'])

        # Converting time_period column to string data type
        df['time_period'] = df['time_period'].astype('string')

        # Converting date_uuid column to string data type
        df['date_uuid'] = df['date_uuid'].astype('string')

        return df
