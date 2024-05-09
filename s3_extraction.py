from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
data_extractor = DataExtractor()
db_connector = DatabaseConnector()
data_cleaner = DataCleaning()



data_extractor = DataExtractor()
df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv')
print(df, "<df")
print(df['weight'].head(50), "<df[weight].head(50)")
print(df['weight'].unique(), "<df[weight].unique()")
print(df.info(), "<df.info()")

df = data_cleaner.convert_product_weights(df)
print(df['weight'].unique(), "<df['weight'].unique()")
print(df.info(), "<df.info()")
print(df['weight'], "<df['weight']")
# print(cleaned_df['weight_(kg)'], "<cleaned_df[weight_kg]")