from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
data_extractor = DataExtractor()
db_connector = DatabaseConnector()
data_cleaner = DataCleaning()


data_extractor = DataExtractor()
db_connector = DatabaseConnector()
yaml_file = 'db_creds.yaml'
table_names = db_connector.list_db_tables(yaml_file)
print(table_names, 'table names')
orders_table_data = data_extractor.read_rds_table(db_connector, yaml_file)
# print(orders_table_data, "<orders_table_data")
# print(orders_table_data.info(), "<orders_table_data.info()")

cleaned_orders = data_cleaner.clean_orders_data(orders_table_data)
# print(cleaned_orders, "<cleaned_orders")
print(cleaned_orders.info(), "<cleaned_orders.info()")

# user_data = data_extractor.read_rds_table(db_connector, yaml_file)
# print(user_data.info(), "<user_data.info()")
# print(user_data.head(100), "<user_data.head(100)")

# cleaned_df = data_cleaner.clean_user_data(user_data)
# print(cleaned_df, "<cleaned_df")
# print(cleaned_df.info(), "cleaned_df.info()")
