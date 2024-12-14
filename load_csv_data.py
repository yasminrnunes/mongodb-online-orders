"""
This module provides functionality to load CSV data into a MongoDB database.

The module reads environment variables to configure the MongoDB connection and CSV file paths.
It includes functions to read CSV files, transform the data, and load it into MongoDB collections.

Functions:
    load(): Main function to load data into the database.
    _read_csv_file(file_name): Reads a CSV file and returns a pandas DataFrame.
    _load_dataframe_mongodb(dataframe, collection): Loads a pandas DataFrame into a MongoDB collection.
    _load_orders_with_products(dataframe, collection): Loads orders with products into a MongoDB collection.

Environment Variables:
    MONGO_URI: MongoDB connection URI.
    MONGODB_NAME: Name of the MongoDB database.
    DELIMITER: Delimiter used in the CSV files.
    CATEGORIES_FILE: Path to the categories CSV file.
    PRODUCT_FILE: Path to the products CSV file.
    ORDER_FILE: Path to the orders CSV file.
"""

import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from dotenv import load_dotenv
from logger import logger

# Load environment variables from the .env file
load_dotenv()

# Define parameters using environment variables
MONGO_URI = os.getenv("MONGO_URI")
MONGODB_NAME = os.getenv("MONGODB_NAME")
DELIMITER = os.getenv("DELIMITER")
CATEGORIES_FILE = os.getenv("CATEGORIES_FILE")
PRODUCT_FILE = os.getenv("PRODUCT_FILE")
ORDER_FILE = os.getenv("ORDER_FILE")


def load():
    logger.info("Loading data into the database")

    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGODB_NAME]

        categories = _read_csv_file(CATEGORIES_FILE)
        _load_dataframe_mongodb(categories, db["categories"])

        products = _read_csv_file(PRODUCT_FILE)
        _load_dataframe_mongodb(products, db["products"])

        orders = _read_csv_file(ORDER_FILE)
        _load_orders_with_products(orders, db["orders"])
    except Exception as e:
        logger.error("An error occurred loading data: %s", e)
    finally:
        if client:
            client.close()


# Function to read csv files
def _read_csv_file(file_name):
    try:
        data = pd.read_csv(file_name, delimiter=DELIMITER)
        return data
    except FileNotFoundError:
        logger.error("Error: The file %s was not found.", file_name)
    except Exception as e:
        logger.error("Reading error: %s", e)


def _load_dataframe_mongodb(dataframe, collection):
    data_dict = dataframe.to_dict("records")
    collection.insert_many(data_dict)
    logger.info("Loading data into the collection %s completed.", collection.name)


def _load_orders_with_products(dataframe, collection):
    # Group the dataframe by order id
    grouped = dataframe.groupby(["id", "customer_id", "date"])

    orders = []
    for (order_id, customer_id, date), group in grouped:
        products = group[
            ["product_id", "product_quantity", "product_unit_value"]
        ].to_dict("records")

        products = [
            {
                k: (
                    Decimal128(str(v).replace(",", "."))
                    if k == "product_unit_value"
                    else int(v) if isinstance(v, (np.int64, np.int32)) else v
                )
                for k, v in product.items()
            }
            for product in products
        ]

        order = {
            "id": int(order_id),
            "customer_id": int(customer_id),
            "date": pd.to_datetime(date),
            "products": products,
        }
        orders.append(order)

    # Insert the orders into the collection
    collection.insert_many(orders)
    logger.info("Loading data into the collection %s completed.", collection.name)
