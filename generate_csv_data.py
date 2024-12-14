"""
This script generates CSV files for orders, categories, and products data.

Functions:
    _categories_data():
        Returns a list of dictionaries containing category data.

    _products_data():
        Returns a list of dictionaries containing product data.

    generate_orders_csv(file_path, num_records):
        Generates a CSV file with order data.
        Args:
            file_path (str): The path to the CSV file to be created.
            num_records (int): The number of order records to generate.

    generate_categories_csv(file_path):
        Generates a CSV file with category data.
        Args:
            file_path (str): The path to the CSV file to be created.

    generate_products_csv(file_path):
        Generates a CSV file with product data.
        Args:
            file_path (str): The path to the CSV file to be created.
"""

import os
import csv
import datetime
from dotenv import load_dotenv
from faker import Faker
from logger import logger

# Load environment variables from the .env file
load_dotenv()

# Define parameters using environment variables
DELIMITER = os.getenv("DELIMITER")


def _categories_data():
    return [
        {"id": 1, "name": "Electronics"},
        {"id": 2, "name": "Books"},
        {"id": 3, "name": "Home & Kitchen"},
        {"id": 4, "name": "Sports"},
        {"id": 5, "name": "Clothing"},
    ]


def _products_data():
    return [
        {"id": 1, "name": "Laptop", "category_id": 1, "price": 1799.99},
        {"id": 2, "name": "Smartphone", "category_id": 1, "price": 150.67},
        {"id": 3, "name": "Headphones", "category_id": 1, "price": 49.12},
        {"id": 4, "name": "Tablet", "category_id": 1, "price": 299.34},
        {"id": 5, "name": "Camera", "category_id": 1, "price": 399.56},
        {"id": 6, "name": "E-book Reader", "category_id": 2, "price": 119.78},
        {"id": 7, "name": "Cookbook", "category_id": 2, "price": 24.34},
        {"id": 8, "name": "Novel", "category_id": 2, "price": 14.67},
        {"id": 9, "name": "Textbook", "category_id": 2, "price": 79.23},
        {"id": 10, "name": "Blender", "category_id": 3, "price": 59.45},
        {"id": 11, "name": "Coffee Maker", "category_id": 3, "price": 99.78},
        {"id": 12, "name": "Microwave", "category_id": 3, "price": 149.34},
        {"id": 13, "name": "Vacuum Cleaner", "category_id": 3, "price": 199.56},
        {"id": 14, "name": "Soccer Ball", "category_id": 4, "price": 29.12},
        {"id": 15, "name": "Tennis Racket", "category_id": 4, "price": 69.34},
        {"id": 16, "name": "Basketball", "category_id": 4, "price": 24.56},
        {"id": 17, "name": "Yoga Mat", "category_id": 4, "price": 19.78},
        {"id": 18, "name": "Running Shoes", "category_id": 4, "price": 89.23},
        {"id": 19, "name": "T-shirt", "category_id": 5, "price": 14.45},
        {"id": 20, "name": "Jeans", "category_id": 5, "price": 39.78},
        {"id": 21, "name": "Jacket", "category_id": 5, "price": 59.34},
        {"id": 22, "name": "Sneakers", "category_id": 5, "price": 49.56},
        {"id": 23, "name": "Dress", "category_id": 5, "price": 69.12},
        {"id": 24, "name": "Smartwatch", "category_id": 1, "price": 249.34},
        {"id": 25, "name": "Monitor", "category_id": 1, "price": 179.56},
        {"id": 26, "name": "Keyboard", "category_id": 1, "price": 39.12},
        {"id": 27, "name": "Mouse", "category_id": 1, "price": 19.34},
        {"id": 28, "name": "Printer", "category_id": 1, "price": 119.56},
        {"id": 29, "name": "Fiction Book", "category_id": 2, "price": 9.78},
        {"id": 30, "name": "Non-fiction Book", "category_id": 2, "price": 19.23},
        {"id": 31, "name": "Mystery Book", "category_id": 2, "price": 14.45},
        {"id": 32, "name": "Science Book", "category_id": 2, "price": 29.78},
        {"id": 33, "name": "Toaster", "category_id": 3, "price": 24.34},
        {"id": 34, "name": "Mixer", "category_id": 3, "price": 44.56},
        {"id": 35, "name": "Air Fryer", "category_id": 3, "price": 99.12},
        {"id": 36, "name": "Grill", "category_id": 3, "price": 149.78},
        {"id": 37, "name": "Dumbbells", "category_id": 4, "price": 49.34},
        {"id": 38, "name": "Treadmill", "category_id": 4, "price": 499.56},
        {"id": 39, "name": "Exercise Bike", "category_id": 4, "price": 299.12},
        {"id": 40, "name": "Golf Clubs", "category_id": 4, "price": 399.34},
        {"id": 41, "name": "Hat", "category_id": 5, "price": 19.56},
        {"id": 42, "name": "Scarf", "category_id": 5, "price": 24.12},
        {"id": 43, "name": "Gloves", "category_id": 5, "price": 14.34},
        {"id": 44, "name": "Socks", "category_id": 5, "price": 9.56},
        {"id": 45, "name": "Belt", "category_id": 5, "price": 29.12},
        {"id": 46, "name": "Smart TV", "category_id": 1, "price": 599.34},
        {"id": 47, "name": "Gaming Console", "category_id": 1, "price": 399.56},
        {"id": 48, "name": "Router", "category_id": 1, "price": 79.12},
        {"id": 49, "name": "External Hard Drive", "category_id": 1, "price": 99.34},
        {"id": 50, "name": "Webcam", "category_id": 1, "price": 69.56},
    ]


def generate_orders_csv(file_path, num_records):
    """
    Generates a CSV file with order data.
    Args:
        file_path (str): The path to the file where the CSV data will be written.
        num_records (int): The number of order records to generate.
    The CSV file will contain the following columns:
        - id: The order ID.
        - customer_id: The ID of the customer who placed the order.
        - date: The date and time when the order was placed.
        - product_id: The ID of the product ordered.
        - product_quantity: The quantity of the product ordered.
        - product_unit_value: The unit price of the product.
    The function uses the Faker library to generate random data for the orders.
    """

    logger.info("Generating orders on file %s", file_path)
    fake = Faker()

    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=DELIMITER)
        writer.writerow(["id", "customer_id", "date", "product_id", "product_quantity", "product_unit_value"])

        # Generate random order data
        for i in range(0, num_records):
            order_id = i
            customer_id = fake.random_int(min=1000, max=6000)
            order_date = (
                fake.date_time_between(
                    start_date=datetime.date(2022, 1, 1), end_date="now"
                ).isoformat(timespec="seconds")
                + "Z"
            )

            # Random number of products in the order
            for _ in range(fake.random_int(min=1, max=3)):  # line products
                product_id = fake.random_int(min=1, max=len(_products_data()))
                product_quantity = fake.random_int(min=1, max=2) # quantity of products
                product_unit_value = _products_data()[product_id - 1]["price"]

                writer.writerow(
                    [
                        order_id,
                        customer_id,
                        order_date,
                        product_id,
                        product_quantity,
                        product_unit_value,
                    ]
                )


def generate_categories_csv(file_path):
    logger.info("Generating categories on file %s", file_path)
    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=DELIMITER)
        writer.writerow(["id", "name"])

        for category in _categories_data():
            writer.writerow([category["id"], category["name"]])


def generate_products_csv(file_path):
    logger.info("Generating products on file %s", file_path)
    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=DELIMITER)
        writer.writerow(["id", "name", "category_id"])

        for product in _products_data():
            writer.writerow([product["id"], product["name"], product["category_id"]])
