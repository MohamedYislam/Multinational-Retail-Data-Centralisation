from ..data_extraction import DataExtractor
from ..database_utils import DatabaseConnector
from ..data_cleaning import DataCleaning

def create_user_table():
    # Initialize components
    data_extractor = DataExtractor()
    db_connector = DatabaseConnector()
    data_cleaner = DataCleaning()

    # Configuration
    yaml_file = 'db_creds.yaml'

    # Data extraction
    table_names = db_connector.list_db_tables(yaml_file)
    print(table_names, 'table names')
    raw_user_data_df = data_extractor.read_rds_table(db_connector, yaml_file)
    print(raw_user_data_df, "<raw_user_data_df")
    print(raw_user_data_df.info(), "<raw_user_data_df.info()")

    # Data cleaning
    cleaned_user_data_df = data_cleaner.clean_user_data(raw_user_data_df)
    print(cleaned_user_data_df, "<cleaned_user_data_df")
    print(cleaned_user_data_df.info(), "<cleaned_user_data_df.info()")

    # Data upload
    upload_success = db_connector.upload_to_db(cleaned_user_data_df, 'sales_db_creds.yaml', 'dim_users')
    if upload_success:
        print("Upload successful")
    else:
        print("Upload failed")

if __name__ == "__main__":
    create_user_table()