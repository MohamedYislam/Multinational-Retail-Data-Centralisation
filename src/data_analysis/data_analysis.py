from ..database_utils import DatabaseConnector
from sqlalchemy import text

# Milestone 4 Task 1

def querying_data_total_no_stores():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:

        
        
        modify_dim_orders_column = text(f"""
            SELECT country_code AS country,
                COUNT(*) AS total_no_stores
            FROM 
                dim_store_details
            GROUP BY 
                country
            ORDER BY 
                total_no_stores DESC;
        """)
        
        # Execute the statement using the connection
        connection.execute(modify_dim_orders_column)
        print("order table column data type altered")  

# Milestone 4 Task 2
def stores_by_locality():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:

        
        
        stores_by_locality_sql_query = text(f"""
            SELECT locality,
                COUNT(*) AS total_no_stores
            FROM 
                dim_store_details
            GROUP BY 
                locality
            ORDER BY 
                total_no_stores DESC;
        """)
        
        # Execute the statement using the connection
        connection.execute(stores_by_locality_sql_query)
        print("order table column data type altered")  


# Milestone 4 Task 3
def sales_by_month():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:
        sales_by_month_query = text(f"""
            SELECT 
                dates.month,
                ROUND(SUM((products."product_price(£)" * orders.product_quantity)::numeric), 2) AS total_sales
            FROM
                orders_table orders
            JOIN
                dim_date_times dates ON orders.date_uuid = dates.date_uuid
            JOIN
                dim_products products ON orders.product_code = products.product_code
            GROUP BY
                dates.month
            ORDER BY
                total_sales DESC;
            """)
            
        # Execute the statement using the connection
        result = connection.execute(sales_by_month_query)
        print("Sales by month are:")
        for row in result:
            print(f"Month: {row[0]}, Total Sales: {row[1]}")


# Milestone 4 Task 4
def online_offline_sales():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:
        online_offline_sales_query = text(f"""
            SELECT 
                COUNT(*) AS numbers_of_sales,
                SUM(product_quantity) AS product_quantity_count,
                CASE 
                    WHEN store_code = 'WEB-1388012W' THEN 'Web'
                    ELSE 'Offline'
                END AS location
            FROM 
                orders_table
            GROUP BY 
                location;
            """)
            
        # Execute the statement using the connection
        result = connection.execute(online_offline_sales_query)
        for row in result:
            print(f"Numbers of Sales: {row[0]}, Product Quantity Count: {row[1]}, Location: {row[2]}")


# Milestone 4 Task 5
def percentage_of_sales_store_type():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:
        percentage_of_sales_store_type_query = text(f"""
            WITH orders_store_products_cte AS (
                SELECT 
                    s.store_type,
                    (o.product_quantity * p."product_price(£)")::numeric AS amount_paid
                FROM
                    orders_table o
                JOIN
                    dim_products p ON p.product_code = o.product_code
                JOIN
                    dim_store_details s ON s.store_code = o.store_code
            )
            SELECT 
                store_type,
                ROUND(SUM(amount_paid), 2) AS total_sales,
                ROUND((SUM(amount_paid) / SUM(SUM(amount_paid)) OVER ()) * 100, 2) AS percentage_total
            FROM 
                orders_store_products_cte
            GROUP BY 
                store_type
            ORDER BY 
                percentage_total DESC;
            """)
            
        # Execute the statement using the connection
        result = connection.execute(percentage_of_sales_store_type_query)
        for row in result:
            print(f"store_type: {row[0]}, total_sales: {row[1]}, percentage_total(%): {row[2]}")


# Milestone 4 Task 6
def highest_selling_month():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:
        highest_selling_month_query = text(f"""
            WITH highest_selling_month_cte AS (
                SELECT DISTINCT ON (d.month)
                    SUM(ROUND((p."product_price(£)" * o.product_quantity)::numeric, 2)) AS total_sales,
                    d.year,
                    d.month
                FROM
                    orders_table o
                JOIN
                    dim_products p ON p.product_code = o.product_code
                JOIN
                    dim_date_times d ON d.date_uuid = o.date_uuid
                GROUP BY
                    d.year,
                    d.month
                ORDER BY
                    d.month DESC,
                    total_sales DESC
            )
            SELECT * 
            FROM 
                highest_selling_month_cte
            ORDER BY
                total_sales DESC;
    """)
            
        # Execute the statement using the connection
        result = connection.execute(highest_selling_month_query)
        print("Highest Selling Month:")
        for row in result:
            print(f"total_sales: {row[0]}, year: {row[1]}, month: {row[2]}")


# Milestone 4 Task 7
def staff_head_count_by_country():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:
        staff_head_count_by_country_query = text(f"""
            SELECT SUM(staff_numbers) AS total_staff_number, 
                country_code 
            FROM 
                dim_store_details 
            GROUP BY 
                country_code 
            ORDER BY 
                total_staff_number DESC;
            """)
            
        # Execute the statement using the connection
        result = connection.execute(staff_head_count_by_country_query)
        print("Staff head count by country:")
        for row in result:
            print(f"total_staff_number: {row[0]}, country_code: {row[1]}")

# Milestone 4 Task 8
def best_selling_german_stores():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:
        best_selling_german_stores_query = text(f"""
            SELECT
                SUM(ROUND((o.product_quantity * p."product_price(£)")::numeric,2)) AS total_sales,
                s.store_type,
                s.country_code
            FROM
                orders_table o
            JOIN
                dim_products p ON p.product_code = o.product_code
            JOIN
                dim_store_details s ON s.store_code = o.store_code
            WHERE
                s.country_code = 'DE'
            GROUP BY
                s.store_type,
                s.country_code
            ORDER BY
                total_sales ASC;
            """)
            
        # Execute the statement using the connection
        result = connection.execute(best_selling_german_stores_query)
        for row in result:
            print(f"total_sales: {row[0]}, store_type: {row[1]}, country_code: {row[2]}")

# Milestone 4 Task 8
def velocity_of_sales_by_year():
    db_connector = DatabaseConnector()
    yaml_file = 'sales_db_creds.yaml'
    engine = db_connector.init_db_engine(yaml_file)
    
    # Obtain a connection from the engine
    with engine.connect() as connection:
        velocity_of_sales_by_year_query = text(f"""
            WITH next_time_stamp_cte AS (
                SELECT
                    d.timestamp AS current_timestamp,
                    d.year,
                    (LEAD(d.timestamp) OVER (ORDER BY d.timestamp)) - d.timestamp AS time_until_next_transaction
                FROM 
                    dim_date_times d
            )
            SELECT 
                year,
                CONCAT(
                    '"hours": ', EXTRACT(HOUR FROM AVG(time_until_next_transaction)), ', ',
                    '"minutes": ', EXTRACT(MINUTE FROM AVG(time_until_next_transaction)), ', ',
                    '"seconds": ', EXTRACT(SECOND FROM AVG(time_until_next_transaction)), ', ',
                    '"milliseconds": ', EXTRACT(MILLISECOND FROM AVG(time_until_next_transaction))
                ) AS actual_time_taken
            FROM
                next_time_stamp_cte
            GROUP BY
            year
            ORDER BY
                AVG(time_until_next_transaction) DESC;
            """)
            
        # Execute the statement using the connection
        result = connection.execute(velocity_of_sales_by_year_query)
        for row in result:
            print(f"year: {row[0]}, actual_time_taken: {row[1]}")            

sales_by_month()
online_offline_sales()
percentage_of_sales_store_type()
staff_head_count_by_country()
highest_selling_month()
best_selling_german_stores()
velocity_of_sales_by_year()
