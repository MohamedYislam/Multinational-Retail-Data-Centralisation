import yaml  
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError, PendingRollbackError

class DatabaseConnector:
    """
    A class for connecting to and interacting with a database.
    """

    def __init__(self):
        pass  # The constructor method is empty, as no initialization is needed

    # Task 3 step 2
    def read_db_creds(self, file_path):  # Define a method to read database credentials from a YAML file
        """
        Read database credentials from a YAML file.

        Args:
            file_path (str): The path to the YAML file containing the database credentials.

        Returns:
            dict: A dictionary containing the database credentials.
        """
        with open(file_path, 'r') as f:  # Open the YAML file in read mode
            creds = yaml.safe_load(f)  # Load the YAML file contents into a dictionary
        return creds  # Return the dictionary containing the database credentials

    # Task 3 step 3
    def init_db_engine(self,yaml_file):  # Define a method to initialize the database engine
        """
        Initialize the database engine using the database credentials.

        Returns:
            sqlalchemy.engine.Engine: The initialized database engine.
        """
        creds = self.read_db_creds(yaml_file)  # Read the database credentials from the YAML file
        engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")  # Create the database engine using the credentials

        return engine  # Return the initialized database engine

    # Task 3 step 4
    def list_db_tables(self, yaml_file):
        """
        List all tables in the database.

        Returns:
            list: A list of table names in the database.
        """
        engine = self.init_db_engine(yaml_file)  # Use the init_db_engine method to get the engine instance
        inspector = inspect(engine)  # Create an inspector object from the engine
        table_names = inspector.get_table_names()  # Get a list of table names
        return table_names

    # Task 3 step 7
    def upload_to_db(self, df, yaml_file, table_name):  # Define a method to upload a DataFrame to a specified table in the database (Task 3, Step 7)
        """
        Upload a DataFrame to a specified table in the database.

        Args:
            df (pandas.DataFrame): The DataFrame to be uploaded.
            table_name (str): The name of the table to upload the DataFrame to.
        """


        engine = self.init_db_engine(yaml_file)  # Initialize the database engine

        try:
            print("inside try")
            # Use the 'to_sql' method to upload the DataFrame to the specified table
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Data uploaded successfully to {table_name} table.")
            return True
        except PendingRollbackError:
            print("Encountered PendingRollbackError. Attempting to rollback the transaction.")
            with engine.connect() as conn:
                conn.execute("ROLLBACK")
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            return True
        except SQLAlchemyError as e:
            print(f"Error uploading data to {table_name} table: {str(e)}")
            return False