from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
data_extractor = DataExtractor()
db_connector = DatabaseConnector()
data_cleaner = DataCleaning()



data_extractor = DataExtractor()
df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv')
# print(df, "<df")
# print(df['weight'].head(50), "<df[weight].head(50)")
# print(df['weight'].unique(), "<df[weight].unique()")
# print(df.info(), "<df.info()")

# df = data_cleaner.convert_product_weights(df)
# print(df['weight(kg)'], "<df['weight']")
# print(df.info(), "<df.info()")
df = data_cleaner.clean_products_data(df)
print(df['product_code'], "<df[product_code]")
print(df['product_code'].unique(), "<unique product_code")
print(df.info(), "<df.info()")


# print(cleaned_df['weight_(kg)'], "<cleaned_df[weight_kg]")