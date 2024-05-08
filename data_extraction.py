import pandas as pd
import requests

from tabula import read_pdf

class DataExtractor:
    """
    A class for extracting data from various sources.
    """

    def __init__(self):
        pass  # The constructor method is empty, as no initialization is needed

    # Task 3 step 5
    def read_rds_table(self, db_connector, yaml_file, table_name = 'legacy_users'):  # Define a method to extract a table from the database into a DataFrame (Task 3, Step 5)
        """
        Extract a table from the database into a pandas DataFrame.

        Args:
            db_connector (DatabaseConnector): An instance of the DatabaseConnector class.
            table_name (str): The name of the table to extract.

        Returns:
            pandas.DataFrame: A DataFrame containing the extracted table data.
        """
        engine = db_connector.init_db_engine(yaml_file)  # Initialize the database engine using the provided DatabaseConnector instance
        df = pd.read_sql_table(table_name, engine)  # Read the specified table from the database into a DataFrame
        return df  # Return the DataFrame containing the extracted table data

    # Task 4 step 2
    def retrieve_pdf_data(self, pdf_link):
        """
        Retrieves data from a PDF file and returns it as a pandas DataFrame.

        Args:
            pdf_link (str): The URL or local path to the PDF file.

        Returns:
            pandas.DataFrame: A DataFrame containing the data extracted from the PDF file.
        """

        dfs = read_pdf(pdf_link, pages='all')
        df = pd.concat(dfs, ignore_index=True)

        return df

    # Task 5 step 1:

    def list_number_of_stores(self, number_stores_endpoint, headers):
        """
        Retrieves the number of stores from the API.

        Args:
            number_stores_endpoint (str): The API endpoint URL to retrieve the number of stores.
            headers (dict): A dictionary containing the API key header.

        Returns:
            int: The number of stores.
        """
        
        response = requests.get(number_stores_endpoint, headers = headers)
        if response.status_code == 200:
            data = response.json()
            return data["number_stores"]
        else:
            return None
        
    def retrieve_store_data(self, store_details_endpoint, number_stores_endpoint, headers):
        """
        Retrieve data for all stores from the provided API endpoints.

        Args:
            store_details_endpoint (str): The API endpoint to retrieve store details.
            number_stores_endpoint (str): The API endpoint to retrieve the number of stores.
            headers (dict): The headers dictionary containing the API key.

        Returns:
            pandas.DataFrame: A DataFrame containing the store data, or None if the request fails.
        """
        
        num_stores = self.list_number_of_stores(number_stores_endpoint, headers)
        print(num_stores, "<num_stores")
        if num_stores is not None:
            store_data = []
            for store_number in range(0, num_stores + 1):
                url = store_details_endpoint.format(store_number=store_number)
                print(url, "<url")
                response = requests.get(url, headers = headers)
                print(response, "<response")
                if response.status_code == 200:
                    store_data.append(response.json())
                else:
                    print(f"Failed to retrieve data for store number {store_number}")
                
            df = pd.DataFrame(store_data)
            return df
        else:
            return None

