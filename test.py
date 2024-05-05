from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd


data_extractor = DataExtractor()
db_connector = DatabaseConnector()
data_cleaner = DataCleaning()

table_names = db_connector.list_db_tables()
print(table_names, 'table names')
user_data = data_extractor.read_rds_table(db_connector)
print(user_data.info(), "<user_data.info()")
print(user_data.head(100), "<user_data.head(100)")

cleaned_df = data_cleaner.clean_user_data(user_data)
print(cleaned_df, "<cleaned_df")
print(cleaned_df.info(), "cleaned_df.info()")


# for table in table_names:
#     user_data = data_extractor.read_rds_table(db_connector, table)
#     print(table, "<table")
#     print(user_data, "<<user_data")
#     print(user_data.info(), "<user_data.info()")

#     if table == 'legacy_user':
#         user_data.head(100)

"""
Changes to make:
For the legacy store table:
1. convert date format from object to date
2. Drop the lat column as it only has 11/451 non null values.
3. Some columns where it has continent, it contains values like 1WZB1TEHL or eeEurope. 
Similarly incorrect opening date

For the legacy user table:
Phone number of varying formats, i want to standardise it +44(0)1144960977 239.711.3836 02984 08192
date is in object format, perhaps i can convert it to datetime For better effiency and easier handling?
phone num
"""