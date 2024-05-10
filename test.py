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
To do: MUST
fix issue with uploading data to database.
1. Must upload my cleaned user data to dim table - Task 3 step 8
2. Must upload my cleaned card data to dim table - Task 4 step 4
3. Must upload my cleaned store data to dim table - Task 5 step 5
4. Must upload my cleaned products data to dim table - Task 6 step 4
5. Must upload my cleaned orders data to dim table - Task 7 step 4
change the class so the yaml file name is only initilised once. 
"""


# pdf.to_datetime('25/12')
# pd.to_datetime(df['expiry_date'], infer_datetime_format=True,errors='coerce')
