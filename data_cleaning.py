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

        # Validating first and last name, and converting column type from ojbect to string.

        df['first_name'] = df['first_name'].apply(lambda x: x if re.match(r'^[a-zA-Z\s-]+$', str(x)) else None)
        df['first_name'] = df['first_name'].astype('string')
        df = df.dropna(subset=['first_name'])

        df['last_name'] = df['last_name'].apply(lambda x : x if re.match(r'^[a-zA-Z\s-]+$', str(x)) else None)
        df['last_name'] = df['last_name'].astype('string')
        df = df.dropna(subset=['last_name'])


        # Convert date_of_birth column to datetime format and removing rows with invalid dates
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], infer_datetime_format=True,errors='coerce')
        df = df.dropna(subset=['date_of_birth'])

        # Converting company name column from object data type to string
        df['company'] = df['company'].astype('string')
        
        # Validating email_address and converting it object type to string
        df['email_address'] = df['email_address'].apply(lambda x: x if validate_email(x) else None)
        df = df.dropna(subset=['email_address'])
        df['email_address'] = df['email_address'].astype('string')

        # Converting address column name from object data type to string
        df['address'] = df['address'].astype('string')

        # Validating country name entry and converting column  to string data type
        df['country'] = df['country'].apply(lambda x: x if pycountry.countries.lookup(x)
                                            or pycountry.countries.get(alpha_2=x.country)
                                            or pycountry.countries.get(alpha_3=x.country) else None)
        df['country'] = df['country'].astype('string')


        # Validating country code
        df['country_code'] = df.apply(lambda x: x.country_code if pycountry.countries.get(alpha_2=x.country_code) else None, axis='columns')
        
        # we check if we are able to look it  up using pycountry, if so then the country_code is valid. else we change it to a null value.

        # Dropping rows with invalid country code
        df = df.dropna(subset=['country_code'])
        
        # Validating phone numbers
        df['phone_number'] = df['phone_number'].apply(lambda x: x if re.match(r'^[^a-zA-Z]*\d{6,}[^a-zA-Z]*$', str(x)) else None)

        # Dropping rows with invalid phone number
        df = df.dropna(subset=['phone_number'])


        # Convert join_date  column to datetime format and removing rows with invalid dates
        df['join_date'] = pd.to_datetime(df['join_date'], infer_datetime_format=True,errors='coerce')
        df = df.dropna(subset=['join_date'])



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
        # removing columns with non-numeric card numbers,  and converting column type from object to integer
        df['card_number'] = pd.to_numeric(df['card_number'], errors='coerce')
        df.dropna(subset=['card_number'], inplace = True)
        df['card_number'] = df['card_number'].astype(int)


        # Convert expiry_date column to datetime format and removing rows with invalid dates
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df.dropna(subset=['expiry_date'], inplace = True)

        # Convert card_provider column from object type to stirng.
        df['card_provider'] = df['card_provider'].astype('string')

        # Convert date_payment_confirmed column to datetime format and removing rows with invalid dates
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], infer_datetime_format=True,errors='coerce')
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
 
        # Converting the longitude column type to numbers, and removing invalid entries
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df.dropna(subset=['longitude'], inplace = True)
        df['longitude'] = df['longitude'].astype(int)

        # Dropping the lat column as it contains many null values.
        df.drop('lat', axis= 'columns', inplace = True)

        # Converting locality column type to string
        df['locality'] = df['locality'].astype('string')

        # Converting store code column type to string
        df['store_code'] = df['store_code'].astype('string')

        # Converting the staff_number column type to numbers, and removing invalid entries
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        df.dropna(subset=['staff_numbers'], inplace = True)
        df['staff_numbers'] = df['staff_numbers'].astype(int)

        # Convert opening_date column to datetime format and removing rows with invalid dates
        df['opening_date'] = pd.to_datetime(df['opening_date'], format='%Y-%m-%d', errors='coerce')
        df = df.dropna(subset=['opening_date'])

        # Converting store type column to string
        df['store_type'] = df['store_type'].astype('string')

        # converting latitude column to float64 type
        df['latitude'] = df['latitude'].astype('float32')

        # Converting country code column to string
        df['country_code'] = df['country_code'].astype('string')

        # Converting continent column type to string, and correcting continent miss spellings.
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

        if 'x' in weight_str:
            print(weight_str, "<weight_str with x ")
            multiplier, unit_str = weight_str.split('x')
            print(multiplier, "<multiplier")
            print(unit_str, "<unit_str")
            multiplier = int(multiplier.strip())
            print(multiplier, "<<multiplier after change")

            value, unit = re.match(r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)', unit_str.strip()).groups()
            print(value, "<value")
            print(unit, "<unit")
            print(unit_mapping.get(unit), "<unit_mapping.get(unit)") # will convert unit to kg standard. for example  if unit is g we get 0.001 
            unit_converted = unit_mapping.get(unit)
            value = float(value)
            new_weight_str = value * multiplier * unit_converted
            print(new_weight_str, "<new_weight_str after multipling")
            return new_weight_str
        else:
            # Use a regular expression to match the weight string
            # The pattern matches a numerical value (with or without a decimal point)
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

            # Return the calculated weight in kilograms
            return new_weight_str

 
    def convert_product_weights(self, df):
        """
        Convert the product weights to decimal values representing their weight in kilograms (kg).

        Args:
            products_df (pandas.DataFrame): The DataFrame containing the product data.

        Returns:
            pandas.DataFrame: The DataFrame with the 'weight' column converted to kilograms.
        """
  
        # Checking rows for invalid entries
        df['weight'] = df['weight'].apply(lambda x: x if re.match(r'^[0-9gGkKmMlLxX\. ]+$', str(x)) else None)
        print(df.info(), "<df.info() here after changes ")
        df.dropna(subset=['weight'], inplace = True)
        print(df.info(), "<df.info() after dropping")
        df['weight'] = df['weight'].apply(self.parse_weight)
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
        df['weight_(kg)'] = df['weight'].apply(self.convert_product_weights)
        return df
