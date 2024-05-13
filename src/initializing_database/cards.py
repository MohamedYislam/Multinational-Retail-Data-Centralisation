from ..data_extraction import DataExtractor
from ..database_utils import DatabaseConnector
from ..data_cleaning import DataCleaning

def create_cards_table():
    data_extractor = DataExtractor()
    db_connector = DatabaseConnector()
    data_cleaner = DataCleaning()

    card_data = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    print(card_data, "<card_data")
    print(card_data.info(), "<card_data.info()")
    cleaned_card_data = data_cleaner.clean_card_data(card_data)
    print(cleaned_card_data, "<cleaned_card_data")
    print(cleaned_card_data.info(), "<cleaned_card_data.info()")

    upload_success_card = db_connector.upload_to_db(cleaned_card_data, 'sales_db_creds.yaml', 'dim_card_details')
    if upload_success_card:
        print("Upload successful")
    else:
        print("Upload failed")

if __name__ == "__main__":
    create_cards_table()