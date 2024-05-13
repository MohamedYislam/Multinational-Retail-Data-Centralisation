from ..data_extraction import DataExtractor
from ..database_utils import DatabaseConnector
from ..data_cleaning import DataCleaning

def create_products_table():
    data_extractor = DataExtractor()
    db_connector = DatabaseConnector()
    data_cleaner = DataCleaning()

    raw_products_data_df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv')
    print(raw_products_data_df, "<raw_products_data_df")
    print(raw_products_data_df.info(), "<raw_products_data_df.info()")

    cleaned_products_data_df = data_cleaner.clean_products_data(raw_products_data_df)
    print(cleaned_products_data_df, "<cleaned_products_data_df")
    print(cleaned_products_data_df.info(), "<cleaned_products_data_df.info()")

    upload_success_products = db_connector.upload_to_db(cleaned_products_data_df, 'sales_db_creds.yaml', 'dim_products')
    if upload_success_products:
        print("Upload successful")
    else:
        print("Upload failed")

if __name__ == "__main__":
    create_products_table()