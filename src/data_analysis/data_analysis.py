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

def querying_data():
    # SELECT country_code AS country,
    #     COUNT(*) AS total_no_stores
    # FROM 
    #     dim_store_details
    # GROUP BY 
    #     country
    # ORDER BY 
    #     total_no_stores DESC;
    pass

sales_by_month()
"""
# The Operations team would like to know which countries we currently operate in 
and which country now has the most stores. 
Perform a query on the database to get the information, it should return the following information:

+----------+-----------------+
| country  | total_no_stores |
+----------+-----------------+
| GB       |             265 |
| DE       |             141 |
| US       |              34 |
+----------+-----------------+
Note: DE is short for Deutschland(Germany)

Milestone 4 Task 2:
The business stakeholders would like to know which locations currently have the most stores.

They would like to close some stores before opening more in other locations.

Find out which locations have the most stores currently. The query should return the following:

+-------------------+-----------------+
|     locality      | total_no_stores |
+-------------------+-----------------+
| Chapletown        |              14 |
| Belper            |              13 |
| Bushley           |              12 |
| Exeter            |              11 |
| High Wycombe      |              10 |
| Arbroath          |              10 |
| Rutherglen        |              10 |
+-------------------+-----------------+

Sales by month:

Query the database to find out which months have produced the most sales. The query should return the following information:

+-------------+-------+
| total_sales | month |
+-------------+-------+
|   673295.68 |     8 |
|   668041.45 |     1 |
|   657335.84 |    10 |
|   650321.43 |     5 |
|   645741.70 |     7 |
|   645463.00 |     3 |

Strategy:
Orders table contains information on date_uuid, product quantity, and product code.
I will join the orders_table with the date_times table using date_uuid so I know the time each transaction was made.
I will then also join it with the dim_products table using the product code , so I know the price of that product.
The relevent information I need from the orders table: index, date_uuid, user_uuid, product_code, product quantity
The relevent informaiton I need form dim_date times is : date_uuid, month
The relevent information I need from dim_products is : product_code, Product_price

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


"""
querying_data()
# modify_orders_table_column_data_type()

