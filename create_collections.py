"""
This script sets up MongoDB collections for an online orders database. It includes the following collections:
- categories
- products
- orders

Each collection is created with specific validation rules defined using MongoDB's JSON schema.

Functions:
- create(): Connects to the MongoDB instance, sets up the collections, and handles any exceptions.
- _setup_categories_collection(db): Sets up the 'categories' collection with the required schema.
- _setup_products_collection(db): Sets up the 'products' collection with the required schema.
- _setup_orders_collection(db): Sets up the 'orders' collection with the required schema.
- _drop_and_create_collection(db, collection_name): Drops an existing collection if it exists and creates a new one.

Environment Variables:
- MONGO_URI: The URI for connecting to the MongoDB instance.
- MONGODB_NAME: The name of the MongoDB database.
"""

import os
from pymongo import MongoClient
from dotenv import load_dotenv
from logger import logger

# Load environment variables from the .env file
load_dotenv()

# Define parameters using environment variables
MONGO_URI = os.getenv("MONGO_URI")
MONGODB_NAME = os.getenv("MONGODB_NAME")


def create():
    """
    Creates the 'online_orders' database and sets up the necessary collections.

    This function connects to the MongoDB instance using the provided URI and database name.
    It then sets up the 'categories', 'products', and 'orders' collections within the database.
    If an error occurs during the process, it logs the error message.

    Raises:
        Exception: If there is an error during the database or collection setup.

    """

    logger.info("Creating 'online_orders' database")

    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGODB_NAME]

        _setup_categories_collection(db)
        _setup_products_collection(db)
        _setup_orders_collection(db)
    except Exception as e:
        logger.error("An error occurred: %s", e)
    finally:
        if client:
            client.close()


def _setup_categories_collection(db):
    _drop_and_create_collection(db, "categories")

    db.command(
        "collMod",
        "categories",
        validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["id", "name"],
                "properties": {
                    "id": {
                        "bsonType": "int",
                        "description": "Unique identifier for the category",
                    },
                    "name": {
                        "bsonType": "string",
                        "description": "Name of the category",
                    },
                },
            }
        },
    )
    logger.info("Collection 'categories' setup completed.")


def _setup_products_collection(db):
    _drop_and_create_collection(db, "products")

    db.command(
        "collMod",
        "products",
        validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["id", "name", "category_id"],
                "properties": {
                    "id": {
                        "bsonType": "int",
                        "description": "Unique identifier for the product",
                    },
                    "name": {
                        "bsonType": "string",
                        "description": "Name of the product",
                    },
                    "category_id": {
                        "bsonType": "int",
                        "description": "Reference to the category id",
                    },
                },
            }
        },
    )
    logger.info("Collection 'products' setup completed.")


def _setup_orders_collection(db):
    _drop_and_create_collection(db, "orders")
    db.command(
        "collMod",
        "orders",
        validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["id", "customer_id", "date", "products"],
                "properties": {
                    "id": {
                        "bsonType": "int",
                        "description": "Unique identifier for the order",
                    },
                    "customer_id": {
                        "bsonType": "int",
                        "description": "Reference to the customer id",
                    },
                    "date": {
                        "bsonType": "date",
                        "description": "Date of the order",
                    },
                    "products": {
                        "bsonType": "array",
                        "description": "List of products in the order",
                        "items": {
                            "bsonType": "object",
                            "required": [
                                "product_id",
                                "product_quantity",
                                "product_unit_value",
                            ],
                            "properties": {
                                "product_id": {
                                    "bsonType": "int",
                                    "description": "Unique identifier of the product",
                                },
                                "product_quantity": {
                                    "bsonType": "int",
                                    "description": "Quantity of the product",
                                },
                                "product_unit_value": {
                                    "bsonType": "decimal",
                                    "description": "Unit price of the product",
                                },
                            },
                        },
                    },
                },
            }
        },
    )

    logger.info("Collection 'orders' setup completed.")


def _drop_and_create_collection(db, collection_name):
    if collection_name in db.list_collection_names():
        logger.debug("Dropping existing '%s' collection", collection_name)
        db.drop_collection(collection_name)

    db.create_collection(collection_name)
