from ..database_utils import DatabaseConnector
from sqlalchemy import text

# Milestone 3 Task 1
def modify_orders_table_column_data_type():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:
        # Find the maximum length of the card_number column
        max_length_card_number_query = text("SELECT MAX(LENGTH(card_number)) FROM orders_table")
        max_length_card_number = connection.execute(max_length_card_number_query).scalar()  # 19
        print(max_length_card_number , "<<max_length_card_number")
        
        # Finding the maximum length of the store_code column
        max_length_store_code_query = text("SELECT MAX(LENGTH(store_code)) FROM orders_table")
        max_length_store_code = connection.execute(max_length_store_code_query).scalar() # 12    

        # Find the maximum length of the product_code column
        max_length_product_code_query = text("SELECT MAX(LENGTH(product_code)) FROM orders_table")
        max_length_product_code = connection.execute(max_length_product_code_query).scalar() # 11

    
        
        modify_dim_orders_column = text(f"""
        ALTER TABLE orders_table
            ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
            ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,               
            ALTER COLUMN card_number TYPE VARCHAR({max_length_card_number}),
            ALTER COLUMN store_code TYPE VARCHAR({max_length_store_code}),
            ALTER COLUMN product_code TYPE VARCHAR({max_length_product_code}),
            ALTER COLUMN product_quantity TYPE SMALLINT;
        """)
        
        # Execute the statement using the connection
        connection.execute(modify_dim_orders_column)
        print("order table column data type altered")    


# Milestone 3 Task 2
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

# Milestone 3 Task 6
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


def modify_dim_card_details_table_column():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:

        # Finding the longest value in the column of each feature, so we can set var(x) to that number
        # So we dont waste any storage space
        
        # Finding the maximum length of card_number 
        max_length_card_number_query = text("SELECT MAX(LENGTH(card_number)) FROM dim_card_details")
        max_length_card_number = connection.execute(max_length_card_number_query).scalar() # 22

        # Finding the maximum length of
        max_length_expiry_date_query = text("SELECT MAX(LENGTH(expiry_date)) FROM dim_card_details")
        max_length_expiry_date = connection.execute(max_length_expiry_date_query).scalar() # 19 

        # Finding maximum length of card_provider 
        max_length_card_provider_query = text("SELECT MAX(LENGTH(card_provider)) FROM dim_card_details")
        max_length_card_provider = connection.execute(max_length_card_provider_query).scalar() # 27 
                 

        alter_dim_card_details_column = text(f"""
            ALTER TABLE dim_card_details
                ALTER COLUMN card_number TYPE VARCHAR({max_length_card_number}),
                ALTER COLUMN expiry_date TYPE VARCHAR({max_length_expiry_date}),
                ALTER COLUMN card_provider TYPE VARCHAR({max_length_card_provider}),
                ALTER COLUMN date_payment_confirmed TYPE DATE;
        """)
        
        # Execute the statement using the connection
        connection.execute(alter_dim_card_details_column)
        print("dim_card_details column data types have been altered")


def creating_schema():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:

        # To ensure referential integrity, we will delete rows from orders_table
        # that have foreign key values not present in the corresponding dimension tables
        cleaning_database = text(f"""
            DELETE FROM orders_table
            WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users)
            OR store_code NOT IN (SELECT store_code FROM dim_store_details)
            OR product_code NOT IN (SELECT product_code FROM dim_products)
            OR date_uuid NOT IN (SELECT date_uuid FROM dim_date_times)
            OR card_number NOT IN (SELECT card_number FROM dim_card_details);
        """)

        removing_reference_keys = text(f"""
                                       
            -- Dropping foreign keys if they exist
            ALTER TABLE IF EXISTS orders_table DROP CONSTRAINT IF EXISTS fk_user_uuid;
            ALTER TABLE IF EXISTS orders_table DROP CONSTRAINT IF EXISTS fk_store_code;
            ALTER TABLE IF EXISTS orders_table DROP CONSTRAINT IF EXISTS fk_product_code;
            ALTER TABLE IF EXISTS orders_table DROP CONSTRAINT IF EXISTS fk_date_uuid;
            ALTER TABLE IF EXISTS orders_table DROP CONSTRAINT IF EXISTS fk_card_number; 

            -- Dropping primary keys if they exist
            ALTER TABLE IF EXISTS orders_table DROP CONSTRAINT IF EXISTS pk_orders_table;
            ALTER TABLE IF EXISTS dim_users DROP CONSTRAINT IF EXISTS pk_dim_users;
            ALTER TABLE IF EXISTS dim_store_details DROP CONSTRAINT IF EXISTS pk_dim_store_details;
            ALTER TABLE IF EXISTS dim_products DROP CONSTRAINT IF EXISTS pk_dim_products;
            ALTER TABLE IF EXISTS dim_date_times DROP CONSTRAINT IF EXISTS pk_dim_date_times;
            ALTER TABLE IF EXISTS dim_card_details DROP CONSTRAINT IF EXISTS pk_dim_card_details;  

        """)
        adding_primary_and_foreign_keys = text(f"""             
            --- Adding primary key to the orders_table
            ALTER TABLE orders_table ADD CONSTRAINT pk_orders_table PRIMARY KEY (index);

            -- Adding primary keys to the dimension tables
            ALTER TABLE dim_users ADD CONSTRAINT pk_dim_users PRIMARY KEY (user_uuid);
            ALTER TABLE dim_store_details ADD CONSTRAINT pk_dim_store_details PRIMARY KEY (store_code);
            ALTER TABLE dim_products ADD CONSTRAINT pk_dim_products PRIMARY KEY (product_code);
            ALTER TABLE dim_date_times ADD CONSTRAINT pk_dim_date_times PRIMARY KEY (date_uuid);
            ALTER TABLE dim_card_details ADD CONSTRAINT pk_dim_card_details PRIMARY KEY (card_number);

            -- adding foreign keys
            ALTER TABLE orders_table ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);
            ALTER TABLE orders_table ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code);
            ALTER TABLE orders_table ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products (product_code);                                           
            ALTER TABLE orders_table ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid);        
            ALTER TABLE orders_table ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);
       
        """)

        # Execute the statement using the connection
        connection.execute(cleaning_database)
        connection.execute(removing_reference_keys)
        connection.execute(adding_primary_and_foreign_keys)
        print("primary and foreign keys have have been added")

def initializing_star_schema():
    modify_orders_table_column_data_type()
    modify_dim_user_table_column_data_type()
    modify_dim_store_details_table_column()
    modify_dim_products()
    modify_dim_date_times_table_column()
    modify_dim_card_details_table_column()
    creating_schema()