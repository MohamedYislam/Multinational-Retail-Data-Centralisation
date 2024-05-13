from ..data_extraction import DataExtractor
from ..database_utils import DatabaseConnector
from ..data_cleaning import DataCleaning

def create_stores_table():
    data_extractor = DataExtractor()
    db_connector = DatabaseConnector()
    data_cleaner = DataCleaning()

    header = {
        "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }
    endpoint_stores_number = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    endpoint_stores_detail = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    stores_number = data_extractor.list_number_of_stores(endpoint_stores_number, header)
    print(stores_number, "<stores_number")

    raw_stores_data_df = data_extractor.retrieve_store_data(endpoint_stores_detail, endpoint_stores_number, header)
    print(raw_stores_data_df, "<raw_stores_data_df")
    print(raw_stores_data_df.info(), "<raw_stores_data_df.info()")
    cleaned_stores_data_df = data_cleaner.clean_store_data(raw_stores_data_df)
    print(cleaned_stores_data_df, "<cleaned_stores_data_df")
    print(cleaned_stores_data_df.info(), "<cleaned_stores_data_df.info()")

    upload_success_stores = db_connector.upload_to_db(cleaned_stores_data_df, 'sales_db_creds.yaml', 'dim_store_details')
    if upload_success_stores:
        print("Upload successful")
    else:
        print("Upload failed")

if __name__ == "__main__":
    create_stores_table()