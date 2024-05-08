from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd



data_extractor = DataExtractor()
db_connector = DatabaseConnector()
data_cleaner = DataCleaning()


# Create the header dictionary
headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
number_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"

# # Call the list_number_of_stores method
# number_of_stores = data_extractor.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", headers)
# print(f"Number of stores: {number_of_stores}")


# """
# To do: MUST
# fix issue with uploading data to database.
# 1. Must upload my cleaned user data to dim table
# 2. Must upload my cleaned card data to dim table


# import requests

# # store_number = 450
# # # 0 to 450 stores exist
# # url = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
# # headers = {
# #     "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
# # }

# # response = requests.get(url, headers=headers)

# # if response.status_code == 200:
# #     data = response.json()
# #     print(data)
# # else:
# #     print(f"Failed to retrieve data for store number {store_number}")


# # Call the retrieve_store_data method with a store number
store_data = data_extractor.retrieve_store_data(store_details_endpoint, number_store_endpoint, headers)

print(store_data.head(20), "<store_data.head(20)")
print(store_data.tail(20), "<store_data.head(20)")
print(store_data.info(), "<store_data.info()")
# # Print the store data if it's not None

print(store_data)
print(store_data.head(20), "<store_data.head(20)")
print(store_data.tail(20), "<store_data.head(20)")
print(store_data.info(), "<store_data.info()")

cleaned_store_data = data_cleaner.clean_store_data(store_data)
print(cleaned_store_data.head(20), "<cleaned_store_data.head(20)")
print(cleaned_store_data.tail(20), "<cleaned_store_data.head(20)")
print(cleaned_store_data.info(), "<cleaned_store_data.info()")



# yaml_file = 'db_creds.yaml'
# table_names = db_connector.list_db_tables(yaml_file)
# print(table_names, 'table names')
# legacy_store_data = data_extractor.read_rds_table(db_connector, yaml_file, "legacy_store_details")

# cleaned_df = data_cleaner.clean_store_data(legacy_store_data)
# print(cleaned_df, "<legacy_store_data")
# print(cleaned_df.info(), "<legacy_store_data.info")
# print(user_data.info(), "<user_data.info()")
# print(user_data.head(100), "<user_data.head(100)")

# cleaned_df = data_cleaner.clean_user_data(user_data)
# print(cleaned_df, "<cleaned_df")
# print(cleaned_df.info(), "cleaned_df.info()")