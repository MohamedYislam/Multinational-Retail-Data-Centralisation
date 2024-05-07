from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
import pycountry


# data_extractor = DataExtractor()
# db_connector = DatabaseConnector()
# yaml_file = 'db_creds.yaml'
# table_names = db_connector.list_db_tables(yaml_file)
# print(table_names, 'table names')
# user_data = data_extractor.read_rds_table(db_connector, yaml_file)
# print(user_data.info(), "<user_data.info()")
# print(user_data.head(100), "<user_data.head(100)")

# cleaned_df = data_cleaner.clean_user_data(user_data)
# print(cleaned_df, "<cleaned_df")
# print(cleaned_df.info(), "cleaned_df.info()")

# # Upload the cleaned data to the 'dim_users' table
# upload_success = db_connector.upload_to_db(cleaned_df, 'sales_db_creds.yaml', 'dim_users')
data_cleaner = DataCleaning()
data_extractor_two = DataExtractor()

card_data = data_extractor_two.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

cleaned_df = data_cleaner.clean_card_data(card_data)

print(cleaned_df.info(), "<cleaned_df.info()")
"""
To do:

change the class so the yaml file name is only initilised once. 
"""