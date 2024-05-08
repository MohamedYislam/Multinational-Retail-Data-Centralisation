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

