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

# MILESTONE 3 TASK 2
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

# MILESTONE 3 TASK 4
def modify_dim_products():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)

    with engine.connect() as connection:
        # Creating new weight class column
        add_weight_class_column = text(f"""
            ALTER TABLE dim_products
                ADD COLUMN weight_class VARCHAR(20);
            """)

        # Filling in the weight_class detail
        update_weight_class_details = text(f"""
            UPDATE dim_products
            SET weight_class = 
                CASE
                    WHEN "weight(kg)" < 2 THEN 'Light'
                    WHEN "weight(kg)" >= 2 AND "weight(kg)" < 40 THEN 'Mid_Sized'
                    WHEN "weight(kg)" >= 40 AND "weight(kg)" < 140 THEN 'Heavy'
                    WHEN "weight(kg)" >= 140 THEN 'Truck_Required'
                END;                           
            """)
        
        # Finding the maximum length of the EAN column 
        max_length_ean_query = text("SELECT MAX(LENGTH(\"EAN\")) FROM dim_products")
        max_length_ean = connection.execute(max_length_ean_query).scalar()

        # Find the maximum length of the product_code column
        max_length_product_code_query = text("SELECT MAX(LENGTH(product_code)) FROM dim_products")
        max_length_product_code = connection.execute(max_length_product_code_query).scalar()


        # Renaming "removed" column to "still_available"
        rename_removed_column = text(f"""
            ALTER TABLE dim_products
                RENAME COLUMN removed TO still_available;
        """)


        # Converting column from text to boolean data type
        convert_removed_column_to_bool = text(f"""                 
            UPDATE dim_products
            SET 
                still_available = 
                CASE
                    WHEN still_available = 'Still_available' THEN TRUE
                    WHEN still_available = 'Removed' THEN FALSE
                END;
        """)

        # Modifying column data type
        modify_dim_products_table_column = text(f"""
            ALTER TABLE dim_products
                ALTER COLUMN "product_price(£)" TYPE FLOAT,
                ALTER COLUMN "weight(kg)" TYPE FLOAT,
                ALTER COLUMN "EAN" TYPE VARCHAR({max_length_ean}),
                ALTER COLUMN product_code TYPE VARCHAR({max_length_product_code}),
                ALTER COLUMN date_added TYPE DATE,
                ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
                ALTER COLUMN still_available TYPE BOOLEAN USING still_available::boolean,
                ALTER COLUMN weight_class TYPE VARCHAR(14);
        """)



        # Execute the statement using the connection
        connection.execute(add_weight_class_column) # Creating new weight class column
        connection.execute(update_weight_class_details) # Categorising weight class between #Light, mid_sized, Heavy and Truck_required
        connection.execute(rename_removed_column) # renaming 'removed' column to 'still_available'      
        connection.execute(convert_removed_column_to_bool) # Converting it to True if still available, otherwise False
        connection.execute(modify_dim_products_table_column) # Changing data types of table column
        
        print("dim_product table has been updated, a new column with weight category has been created")

def modify_dim_date_times_table_column():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:

        # Find the maximum text length of the 'time_period' column
        max_length_time_period_query = text("SELECT MAX(LENGTH(time_period)) FROM dim_date_times")
        max_length_time_period = connection.execute(max_length_time_period_query).scalar()
                

        modify_dim_date_times_column = text(f"""
            ALTER TABLE dim_date_times
                ALTER COLUMN day TYPE VARCHAR(2),
                ALTER COLUMN month TYPE VARCHAR(2),
                ALTER COLUMN year TYPE VARCHAR(4),
                ALTER COLUMN time_period TYPE VARCHAR({max_length_time_period}),
                ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
        """)
        
        # Execute the statement using the connection
        connection.execute(modify_dim_date_times_column)
        print("dim_date_times column data type has been altered")

def modify_dim_date_times_table_column():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:

        # Find the maximum text length of the 'time_period' column
        max_length_time_period_query = text("SELECT MAX(LENGTH(time_period)) FROM dim_date_times")
        max_length_time_period = connection.execute(max_length_time_period_query).scalar()
                

        modify_dim_date_times_column = text(f"""
            ALTER TABLE dim_date_times
                ALTER COLUMN day TYPE VARCHAR(2),
                ALTER COLUMN month TYPE VARCHAR(2),
                ALTER COLUMN year TYPE VARCHAR(4),
                ALTER COLUMN time_period TYPE VARCHAR(10),
                ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
        """)
        
        # Execute the statement using the connection
        connection.execute(modify_dim_date_times_column)
        print("dim_card_details column data types have been altered")

def modify_dim_card_details_table_column():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:

        # Find the maximum text length of the 'time_period' column
        max_length_time_period_query = text("SELECT MAX(LENGTH(time_period)) FROM dim_date_times")
        max_length_time_period = connection.execute(max_length_time_period_query).scalar()
                

        alter_dim_card_details_column = text(f"""
            ALTER TABLE dim_card_details
                ALTER COLUMN card_number TYPE VARCHAR(19),
                ALTER COLUMN expiry_date TYPE VARCHAR(19),
                ALTER COLUMN card_provider TYPE VARCHAR(27),
                ALTER COLUMN date_payment_confirmed TYPE DATE;
        """)
        
        # Execute the statement using the connection
        connection.execute(alter_dim_card_details_column)
        print("dim_card_details column data types have been altered")

modify_dim_user_table_column_data_type()
modify_dim_store_details_table_column()
modify_dim_products()
modify_dim_date_times_table_column()
modify_dim_card_details_table_column()