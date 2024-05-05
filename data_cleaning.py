import pandas as pd

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

        # Convert date_of_birth column to datetime format
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')

        # Drop rows with remaining invalid dates
        df = df.dropna(subset=['date_of_birth'])

        



        # df = df.dropna(how='all')  # Remove rows with all NULL values
        
        # date_cols = ['created_at', 'updated_at']  # Specify the date columns to be converted
        # for col in date_cols:  # Iterate over the date columns
        #     df[col] = pd.to_datetime(df[col], errors='coerce')  # Convert each date column to datetime, coercing errors to NaT

        # Add more cleaning logic here as needed
        
        return df  # Return the cleaned user data DataFrame
