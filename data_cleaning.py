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

    def validate_phone(self, phone_number):
        """
        Helper function to validate the phone number format using regex.

        Args:
            phone_number (str): The phone number to be validated.

        Returns:
            bool: True if the phone number is valid, False otherwise.
        """
        pattern = r'^[^a-zA-Z]*\d{6,}[^a-zA-Z]*$'
        
        if re.match(pattern, phone_number):
            return True
        else:
            return False

        


    # Task 3 step 6
    def clean_user_data(self, df):  # Define a method to clean the user data DataFrame (Task 3, Step 6)
        """
        Clean the user data DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame containing user data to be cleaned.

        Returns:
            pandas.DataFrame: The cleaned user data DataFrame.
        """


        # Convert date_of_birth column to datetime format
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], infer_datetime_format=True,errors='coerce')

        # Drop rows with remaining invalid dates
        df = df.dropna(subset=['date_of_birth'])
        
        # Validating country code
        df['country_code'] = df.apply(lambda x: x.country_code if pycountry.countries.get(alpha_2=x.country_code) else None, axis='columns')
        
        # we check if we are able to look it  up using pycountry, if so then the country_code is valid. else we change it to a null value.

        # Dropping rows with invalid country code
        df = df.dropna(subset=['country_code'])
        
        # Validating phone numbers
        df['phone_number'] = df['phone_number'].apply(lambda x: x if re.match(r'^[^a-zA-Z]*\d{6,}[^a-zA-Z]*$', str(x)) else None)

        # Dropping rows with invalid phone number
        df = df.dropna(subset=['phone_number'])

        # validating emails 
        # df1 = df[(df['euro'] > 100) & df['email'].apply(validate_email)]
        # print (df1)

        # validating email_address and converting it object type to string
        df['email_address'] = df['email_address'].apply(lambda x: x if validate_email(x) else None)
        df = df.dropna(subset=['email_address'])
        df['email_address'] = df['email_address'].astype('string')

        # Validating first and last name, and converting it to string.

        df['first_name'] = df['first_name'].apply(lambda x: x if re.match(r'^[a-zA-Z\s-]+$', str(x)) else None)
        df['first_name'] = df['first_name'].astype('string')
        df = df.dropna(subset=['first_name'])

        df['last_name'] = df['last_name'].apply(lambda x : x if re.match(r'^[a-zA-Z\s-]+$', str(x)) else None)
        df['last_name'] = df['last_name'].astype('string')
        df = df.dropna(subset=['last_name'])



        return df  # Return the cleaned user data DataFrame
