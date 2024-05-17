from .orders import create_orders_table
from .products import create_products_table
from .stores import create_stores_table
from .users import create_user_table
from .date_events import create_date_events_table
from .cards import create_cards_table
from .star_schema_setup import initializing_star_schema

def main():
    print("Starting database initialization and data processing...")
    create_orders_table()
    create_products_table()
    create_stores_table()
    create_user_table()
    create_date_events_table()
    create_cards_table()
    initializing_star_schema()
    print("All data processed and loaded successfully.")

if __name__ == "__main__":
    main()