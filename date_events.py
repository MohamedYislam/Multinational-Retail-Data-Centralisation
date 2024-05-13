from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
data_extractor = DataExtractor()
db_connector = DatabaseConnector()
data_cleaner = DataCleaning()


raw_date_events_data_df = data_extractor.extract_from_s3('s3://data-handling-public/date_details.json', 'json')
print(raw_date_events_data_df, "<raw_date_events_data_df")
print(raw_date_events_data_df.info(), "<raw_date_events_data_df.info()")

cleaned_date_events_data_df = data_cleaner.clean_date_events_data(raw_date_events_data_df)
print(cleaned_date_events_data_df, "<cleaned_date_events_data")
print(cleaned_date_events_data_df.info(), "<cleaned_date_events_data")
# s3://data-handling-public/date_details.json

# print(df['weight'].head(50), "<df[weight].head(50)")
# print(df['weight'].unique(), "<df[weight].unique()")

# df = data_cleaner.convert_product_weights(df)
# print(df['weight(kg)'], "<df['weight']")
# print(df.info(), "<df.info()")
# df = data_cleaner.clean_products_data(df)
# print(df['product_code'], "<df[product_code]")
# print(df['product_code'].unique(), "<unique product_code")
# print(df.info(), "<df.info()")

upload_success_products = db_connector.upload_to_db(cleaned_date_events_data_df, 'sales_db_creds.yaml', 'dim_date_times')
# DO THE ORDERS TABLE STRAIGHT AFTER