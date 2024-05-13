from ..data_extraction import DataExtractor
from ..database_utils import DatabaseConnector
from ..data_cleaning import DataCleaning

def create_date_events_table():
    data_extractor = DataExtractor()
    db_connector = DatabaseConnector()
    data_cleaner = DataCleaning()

    raw_date_events_data_df = data_extractor.extract_from_s3('s3://data-handling-public/date_details.json', 'json')
    print(raw_date_events_data_df, "<raw_date_events_data_df")
    print(raw_date_events_data_df.info(), "<raw_date_events_data_df.info()")

    cleaned_date_events_data_df = data_cleaner.clean_date_events_data(raw_date_events_data_df)
    print(cleaned_date_events_data_df, "<cleaned_date_events_data_df")
    print(cleaned_date_events_data_df.info(), "<cleaned_date_events_data_df.info()")

    upload_success_date_events = db_connector.upload_to_db(cleaned_date_events_data_df, 'sales_db_creds.yaml', 'dim_date_times')
    if upload_success_date_events:
        print("Upload successful")
    else:
        print("Upload failed")

if __name__ == "__main__":
    create_date_events_table()