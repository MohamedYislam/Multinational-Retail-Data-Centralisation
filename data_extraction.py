import pandas as pd 
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
        
        print(df, "<df")
        print(df.info(), "<df.info()")


        return df


