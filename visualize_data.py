"""
This module provides functions to visualize data from a MongoDB database using matplotlib and pandas.

Functions:
- barchart_top_customers(): Generates a bar chart of the top 5 customers by total purchase value in 2024.
- table_top_products_by_category(): Generates a table of the top 3 products by category.
- linechart_sales_by_year(): Generates a line chart of total sales by year.
- linechart_sales_by_month(): Generates a line chart of monthly total sales in 2024.

Dependencies:
- os
- pymongo.MongoClient
- matplotlib.pyplot
- pandas
- dotenv.load_dotenv
"""

import os
from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Define parameters using environment variables
MONGO_URI = os.getenv("MONGO_URI")
MONGODB_NAME = os.getenv("MONGODB_NAME")

# Parameters
_color = "palevioletred"

# Conexión a MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGODB_NAME]
categories_collection = db["categories"]
orders_collection = db["orders"]


def barchart_top_customers():
    # Aggregation pipeline to get the top 5 customers by total purchase value in 2024
    pipeline_top_customers = [
        {"$unwind": "$products"},
        {
            "$addFields": {
                "total_value": {
                    "$multiply": [
                        "$products.product_quantity",
                        "$products.product_unit_value",
                    ]
                },
                "year": {"$year": "$date"},
            }
        },
        {"$match": {"year": 2024}},
        {
            "$group": {
                "_id": "$customer_id",  # Group by customer_id
                "total_sales": {"$sum": "$total_value"},
            }
        },
        {"$sort": {"total_sales": -1}},
        {"$limit": 5},
    ]

    # Execute the pipeline
    top_customers = list(orders_collection.aggregate(pipeline_top_customers))

    # Extract customer IDs and total purchase values
    customer_id = [str(result["_id"]) for result in top_customers]
    total_sales = [
        round(result["total_sales"].to_decimal(), 0) for result in top_customers
    ]

    # Plot the bar chart
    barchart = plt.bar(customer_id, total_sales, color=_color)

    # Bar chart setup
    for bar, bar_value in zip(barchart, total_sales):
        bar_height = bar.get_height()
        formatted_value = "{:,}".format(
            bar_value
        )  # Format value with thousand separator
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar_height,
            f"{formatted_value}",
            ha="center",
            va="bottom",
            fontsize=8,
        )

    plt.xlabel("Customer id")
    plt.ylabel("Total sales")
    plt.title("Top 5 customers by total sales in 2024")
    plt.yticks([])

    for spine in plt.gca().spines.values():
        spine.set_visible(False)

    # Show the plot
    plt.show()


def table_top_products_by_category():
    # Aggregation pipeline to get the top 3 products by category
    pipeline_top_product_category = [
        {"$unwind": "$products"},
        {"$addFields": {"total_quantity": "$products.product_quantity"}},
        {
            "$lookup": {
                "from": "products",  
                "localField": "products.product_id",  
                "foreignField": "id",  
                "as": "product_info",  
            }
        },
        {"$unwind": "$product_info"},
        {
            "$lookup": {
                "from": "categories",  
                "localField": "product_info.category_id",  
                "foreignField": "id",  
                "as": "category_info",  
            }
        },
        {"$unwind": "$category_info"},
        {
            "$group": {
                "_id": {
                    "product_id": "$products.product_id",
                    "category_id": "$product_info.category_id",
                },
                "total_quantity": {"$sum": "$total_quantity"},
                "product_name": {"$first": "$product_info.name"},
                "category_name": {"$first": "$category_info.name"},
            }
        },
        {
            "$sort": {
                "_id.category_id": 1,
                "total_quantity": -1,
            }
        },
        {
            "$setWindowFields": {
                "partitionBy": "$_id.category_id",
                "sortBy": {"total_quantity": -1},
                "output": {"rank": {"$rank": {}}},
            }
        },
        {"$match": {"rank": {"$lte": 3}}},  
        {
            "$project": {
                "_id": 0,  
                "product_name": 1,
                "category_name": 1,
                "total_quantity": 1,
                "rank": 1,
            }
        },
    ]

    # Execute the pipeline and convert the results into a DataFrame
    top_product_category = list(
        orders_collection.aggregate(pipeline_top_product_category)
    )

    df = pd.DataFrame(top_product_category)

    # Sort and rearrange DataFrame
    df_sorted = df.sort_values(by=["category_name", "rank"])
    columns_order = ["rank", "category_name", "product_name", "total_quantity"]
    df_sorted = df_sorted[columns_order]

    # Display the table
    fig, ax = plt.subplots(figsize=(12, 4))  # Tamanho da figura
    ax.axis("off")
    table = ax.table(
        cellText=df_sorted.values,
        colLabels=df_sorted.columns,
        cellLoc="center",
        loc="center",
    )

    # Table style
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.auto_set_column_width(col=list(range(len(df_sorted.columns))))
    for key, cell in table.get_celld().items():
        row, col = key
        if row == 0:  # header row
            cell.set_fontsize(10)
            cell.set_text_props(weight="bold", color="white")
            cell.set_facecolor(_color)

    plt.show()


def linechart_sales_by_year():
    # Aggregation pipeline to calculate total sales by year
    pipeline_sales_year = [
        {"$unwind": "$products"},
        {
            "$addFields": {
                "total_value": {
                    "$multiply": [
                        "$products.product_quantity",
                        "$products.product_unit_value",
                    ]
                },
            }
        },
        {"$addFields": {"year": {"$year": "$date"}}},
        {
            "$group": {
                "_id": "$year",
                "total_sales": {"$sum": "$total_value"},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # Execute the pipeline
    sales_year = list(orders_collection.aggregate(pipeline_sales_year))

    # Prepare data for visualization
    years = [result["_id"] for result in sales_year]  # month
    sales_value = [
        round(result["total_sales"].to_decimal() / 1000, 0)
        for result in sales_year
    ]

    # Plot the line chart
    plt.figure(figsize=(10, 6))
    plt.plot(years, sales_value, marker="o", color=_color)

    # Add data labels
    for m, s in zip(years, sales_value):
        plt.text(m, s, f"{s}k", fontsize=8, ha="right", va="bottom")

    # Chart setting
    plt.xlabel("Years")
    plt.ylabel("Total sales (k euros)")
    plt.title("Total sales by year")
    plt.yticks([])
    plt.xticks(years)
    plt.ylim(bottom=float(min(sales_value)) * 0.9, top=float(max(sales_value)) * 1.1)

    for spine in plt.gca().spines.values():
        spine.set_visible(False)

    plt.show()


def linechart_sales_by_month():
    # Aggregation pipeline to calculate monthly sales for 2024
    pipeline_sales_month = [
        {"$unwind": "$products"},
        {
            "$addFields": {
                "total_value": {
                    "$multiply": [
                        "$products.product_quantity",
                        "$products.product_unit_value",
                    ]
                },
                "month": {"$month": "$date"},
                "year": {"$year": "$date"},
            }
        },
        {"$match": {"year": 2024}},
        {
            "$group": {
                "_id": "$month",
                "total_sales": {"$sum": "$total_value"},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    # Execute the pipeline
    sales_month_2024 = list(orders_collection.aggregate(pipeline_sales_month))

    # Prepare data for visualization
    month = [result["_id"] for result in sales_month_2024]  # month
    sales_month = [
        round(result["total_sales"].to_decimal() / 1000, 0)
        for result in sales_month_2024
    ]

    # Plot the line chart
    plt.figure(figsize=(10, 6))
    plt.plot(month, sales_month, marker="o", color=_color)

    # Add data labels
    for m, s in zip(month, sales_month):
        plt.text(m, s, f"{s}k", fontsize=8, ha="right", va="bottom")

    # Chart settings
    plt.xlabel("Month")
    plt.ylabel("Total sales (k euros)")
    plt.title("Monthly total sales in 2024")
    plt.yticks([])
    plt.xticks(month)
    for spine in plt.gca().spines.values():
        spine.set_visible(False)

    plt.show()
