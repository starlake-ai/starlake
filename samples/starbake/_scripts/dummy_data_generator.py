import glob
import json
import os
import random
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker
import pathlib

data_dir = str(pathlib.Path(__file__).parent.resolve().parent)+'/datasets/incoming/starbake'

fake = Faker()

# Customers
customer_cols = ["customer_id", "first_name", "last_name", "email", "join_date"]
customer_rows = []

# Products
product_cols = ["product_id", "name", "details", "ingredients"]
product_rows = []

# Ingredients
ingredient_cols = ["ingredient_id", "name", "price", "quantity_in_stock"]
ingredient_rows = []

# Orders
order_cols = ["order_id", "customer_id", "timestamp", "status", "products"]
order_rows = []

# Function to generate random dates
def random_dates(start, end, n=10):
    start_u = start.value//10**9
    end_u = end.value//10**9
    return pd.to_datetime(np.random.randint(start_u, end_u, n), unit='s')

# Function to generate random price
def random_price(start, end):
    return round(random.uniform(start, end), 2)

# Function to generate random quantity in stock
def random_quantity(start, end):
    return round(random.uniform(start, end), 2)

# Clear the directory for new files
for filename in glob.glob(f"{data_dir}/**/*"):
    os.remove(filename)

# Function to reset global variables
def reset_rows():
    global customer_rows, ingredient_rows, product_rows, order_rows
    customer_rows = []
    ingredient_rows = []
    product_rows = []
    order_rows = []

# Initialize ids
customer_id = 1
ingredient_id = 1
product_id = 1
order_id = 1

# Update functions to generate incremental ids
def generate_customer_data(n):
    global customer_id, customer_rows
    for _ in range(n):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email()
        join_date = str(fake.date_between(start_date='-1y', end_date='today'))
        customer_rows.append([customer_id, first_name, last_name, email, join_date])
        customer_id += 1

def generate_order_data(n):
    global order_id, order_rows
    for _ in range(n):
        customer = random.choice(customer_rows)
        timestamp = str(fake.date_time_between(start_date='-30d', end_date='now', tzinfo=None))
        status = random.choice(['placed', 'shipped', 'delivered'])
        products = random.sample(product_rows, k=min(3, len(product_rows)))
        products = [{"product_id": prod[0], "quantity": random.randint(1, 5), "price": prod[2]["price"]} for prod in products]
        order_rows.append([order_id, customer[0], timestamp, status, products])
        order_id += 1
# Update product and ingredient lists to be more related to bakery products
bakery_products = ['Chocolate Cake', 'Vanilla Cupcake', 'Blueberry Muffin', 'Cinnamon Roll', 'Baguette', 'Croissant', 'Apple Pie', 'Brioche', 'Sourdough Bread', 'Raspberry Tart']
bakery_ingredients = ['Flour', 'Sugar', 'Eggs', 'Milk', 'Butter', 'Baking Powder', 'Vanilla Extract', 'Cocoa Powder', 'Cinnamon', 'Blueberries', 'Raspberries', 'Apples', 'Yeast', 'Salt']

# Update functions to generate relevant product and ingredient names
def generate_ingredient_data(n):
    global ingredient_id, ingredient_rows
    for _ in range(n):
        name = random.choice(bakery_ingredients)
        bakery_ingredients.remove(name)  # Make sure we don't repeat ingredients
        price = random_price(0.5, 10.0)
        quantity_in_stock = random_quantity(10.0, 100.0)
        ingredient_rows.append([ingredient_id, name, price, quantity_in_stock])
        ingredient_id += 1

def generate_product_data(n):
    global product_id, product_rows
    for _ in range(n):
        name = random.choice(bakery_products)
        bakery_products.remove(name)  # Make sure we don't repeat products
        details = {
            "price": random_price(1.0, 50.0),
            "description": f"This delicious {name} is made with the finest ingredients and baked to perfection.",
            "category": random.choice(['bread', 'cake', 'pastry'])
        }
        ingredients = random.sample(ingredient_rows, k=min(5, len(ingredient_rows)))
        ingredients = [{"ingredient_id": ing[0], "quantity": random_quantity(0.1, 5.0)} for ing in ingredients]
        product_rows.append([product_id, name, details, ingredients])
        product_id += 1

# Generate data
for i in range(1, 6):
    # Reset product and ingredient lists for each iteration
    bakery_products = ['Chocolate Cake', 'Vanilla Cupcake', 'Blueberry Muffin', 'Cinnamon Roll', 'Baguette', 'Croissant', 'Apple Pie', 'Brioche', 'Sourdough Bread', 'Raspberry Tart']
    bakery_ingredients = ['Flour', 'Sugar', 'Eggs', 'Milk', 'Butter', 'Baking Powder', 'Vanilla Extract', 'Cocoa Powder', 'Cinnamon', 'Blueberries', 'Raspberries', 'Apples', 'Yeast', 'Salt']

    generate_customer_data(5)
    generate_ingredient_data(5)
    generate_product_data(5)
    generate_order_data(10)

    dir_path = f"{data_dir}/day_{i}"
    # create data_dir if not exist
    Path(dir_path).mkdir(parents=True, exist_ok=True)

    # Save to files
    df_customers = pd.DataFrame(customer_rows, columns=customer_cols)
    df_customers.to_csv(f"{dir_path}/customers_{i}.csv", index=False)

    df_ingredients = pd.DataFrame(ingredient_rows, columns=ingredient_cols)
    df_ingredients.to_csv(f"{dir_path}/ingredients_{i}.tsv", sep='\t', index=False)

    with open(f"{dir_path}/products_{i}.json", 'w') as f:
        for row in product_rows:
            json.dump({
                "product_id": row[0],
                "name": row[1],
                "details": row[2],
                "ingredients": row[3]
            }, f)
            f.write('\n')

    with open(f"{dir_path}/orders_{i}.json", 'w') as f:
        orders = []
        for row in order_rows:
            orders.append({
                "order_id": row[0],
                "customer_id": row[1],
                "timestamp": row[2],
                "status": row[3],
                "products": row[4]
            })

        json.dump(orders, f, indent=4)

    # Clear rows for next iteration
    reset_rows()
