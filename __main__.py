import os
from load_csv_data import load
from create_collections import create
from generate_csv_data import (
    generate_categories_csv,
    generate_products_csv,
    generate_orders_csv,
)
from visualize_data import (
    barchart_top_customers,
    table_top_products_by_category,
    linechart_sales_by_year,
    linechart_sales_by_month,
)
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Define parameters using environment variables
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE")
PRODUCT_FILE = os.getenv("PRODUCT_FILE")
ORDER_FILE = os.getenv("ORDER_FILE")

# Run the database setup
if __name__ == "__main__":
    # Generate the CSV files
    generate_categories_csv(CATEGORIES_FILE)
    generate_products_csv(PRODUCT_FILE)
    generate_orders_csv(ORDER_FILE, 5000)


    # Create the collections in the database
    create()


    # Load the data into the collections
    load()


    # visualize the data
    # Top 5 Customers by Total Purchase Value
    barchart_top_customers()

    # Top 3 product sold in each category
    table_top_products_by_category()

    # Sales by year
    linechart_sales_by_year()

    # Sales by month in 2024
    linechart_sales_by_month()
