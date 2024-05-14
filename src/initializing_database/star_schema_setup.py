from ..database_utils import DatabaseConnector
from sqlalchemy import text

class StarSchemaSetup:
    def __init__(self, yaml_file):
        self.db_connector = DatabaseConnector()
        self.engine = self.db_connector.init_db_engine(yaml_file)

    def create_fact_sales_table(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_sales (
            sale_id INT PRIMARY KEY,
            product_id INT,
            store_id INT,
            date_id INT,
            quantity_sold INT,
            total_sales FLOAT
        );
        """
        self.engine.execute(create_table_sql)
        print("Fact sales table created.")

    def create_dimension_tables(self):
        # Example SQL for creating a dimension table
        create_dim_product_sql = """
        CREATE TABLE IF NOT EXISTS dim_product (
            product_id INT PRIMARY KEY,
            product_name VARCHAR(255),
            category VARCHAR(100)
        );
        """
        self.engine.execute(create_dim_product_sql)
        print("Dimension product table created.")

    def modify_column_types(self):
        alter_column_sql = "ALTER TABLE fact_sales ALTER COLUMN total_sales TYPE NUMERIC(10, 2);"
        self.engine.execute(alter_column_sql)
        print("Column types modified.")
    
# def modify_dim_user_table_column_data_type():
#     db_connector = DatabaseConnector()
#     yaml_file = 'sales_db_creds.yaml'
#     engine = db_connector.init_db_engine(yaml_file)
    
#     # Obtain a connection from the engine
#     with engine.connect() as connection:
#         modify_dim_user_column = """
#         ALTER TABLE dim_users
#             ALTER COLUMN first_name TYPE VARCHAR(255),
#             ALTER COLUMN last_name TYPE VARCHAR(255),
#             ALTER COLUMN date_of_birth TYPE DATE,
#             ALTER COLUMN country_code TYPE VARCHAR(50),
#             ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
#             ALTER COLUMN join_date TYPE DATE;
#         """
#         # Execute the statement using the connection
#         connection.execute(text(modify_dim_user_column))
#         print("dim user column data type altered")

# StarSchemaSetup.modify_dim_user_table_column_data_type();
# my_instance = StarSchemaSetup('sales_db_creds.yaml')
# my_instance.modify_dim_user_table_column_data_type()
def modify_dim_user_table_column_data_type():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:
        # Find the maximum length of the country_code column
        max_length_query = text("SELECT MAX(LENGTH(country_code)) FROM dim_users")
        max_length = connection.execute(max_length_query).scalar()
        
        modify_dim_user_column = text(f"""
        ALTER TABLE dim_users
            ALTER COLUMN first_name TYPE VARCHAR(255),
            ALTER COLUMN last_name TYPE VARCHAR(255),
            ALTER COLUMN date_of_birth TYPE DATE,
            ALTER COLUMN country_code TYPE VARCHAR({max_length}),
            ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
            ALTER COLUMN join_date TYPE DATE;
        """)
        
        # Execute the statement using the connection
        connection.execute(modify_dim_user_column)
        print("dim user column data type altered")


# MILESTONE 3 TASK 3
def modify_dim_store_details_table_column():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)

    with engine.connect() as connection:
        # Finding the maximum length of the store_code column
        max_length_store_code_query = text("SELECT MAX(LENGTH(store_code)) FROM dim_store_details")
        max_length_store_code = connection.execute(max_length_store_code_query).scalar()

        # Find the maximum length of the country_code column
        max_length_country_code_query = text("SELECT MAX(LENGTH(country_code)) FROM dim_store_details")
        max_length_country_code = connection.execute(max_length_country_code_query).scalar()

        modify_dim_store_table_column = text(f"""
            ALTER TABLE dim_store_details
                ALTER COLUMN longitude TYPE FLOAT,
                ALTER COLUMN locality TYPE VARCHAR(255),
                ALTER COLUMN store_code TYPE VARCHAR({max_length_store_code}),
                ALTER COLUMN staff_numbers TYPE SMALLINT,
                ALTER COLUMN opening_date TYPE DATE,
                ALTER COLUMN store_type DROP NOT NULL,
                ALTER COLUMN store_type TYPE VARCHAR(255),
                ALTER COLUMN latitude TYPE FLOAT,
                ALTER COLUMN country_code TYPE VARCHAR({max_length_country_code}),
                ALTER COLUMN continent TYPE VARCHAR(255);
        """)

        # Updating row of business website

        update_dim_store_row = text(f"""
        UPDATE dim_store_details
            SET 
                locality = NULL
            WHERE
                locality = 'N/A';
        """)

        # Execute the statement using the connection
        connection.execute(modify_dim_store_table_column)
        connection.execute(update_dim_store_row)

        print("dim_store_details column data type altered")


modify_dim_user_table_column_data_type()
modify_dim_store_details_table_column()