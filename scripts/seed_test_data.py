#!/usr/bin/env python3
"""
Seed realistic mock data into PostgreSQL, MySQL, and MongoDB for pipeline testing.
Simulates real-world scenarios: users, orders, products, customers.
Run after docker-compose.test.yml databases are up.
Uses ports: Postgres 15432, MySQL 13306, MongoDB 27018
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

# Add parent so we can import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Default to test ports (override via env for different setups)
PG_PORT = int(os.getenv("PG_TEST_PORT", "15432"))
MYSQL_PORT = int(os.getenv("MYSQL_TEST_PORT", "13306"))
MONGO_PORT = int(os.getenv("MONGO_TEST_PORT", "27018"))

# Realistic mock data for users (name, email)
MOCK_USERS = [
    ("Alice Johnson", "alice.johnson@acme.com"),
    ("Bob Smith", "bob.smith@techcorp.io"),
    ("Carol Williams", "carol.w@retail.co"),
    ("David Brown", "d.brown@startup.io"),
    ("Eva Martinez", "eva.martinez@design.com"),
    ("Frank Chen", "frank.chen@finance.com"),
    ("Grace Lee", "grace.lee@health.org"),
    ("Henry Wilson", "henry.w@logistics.com"),
    ("Ivy Davis", "ivy.davis@edu.edu"),
    ("Jack Taylor", "jack.taylor@media.net"),
    ("Kate Anderson", "kate.anderson@travel.com"),
    ("Leo Thompson", "leo.thompson@food.com"),
    ("Mia Garcia", "mia.garcia@fashion.co"),
    ("Noah White", "noah.white@auto.com"),
    ("Olivia Harris", "olivia.harris@energy.com"),
    ("Paul Clark", "paul.clark@insurance.com"),
    ("Quinn Lewis", "quinn.lewis@software.io"),
    ("Rachel King", "rachel.king@consulting.com"),
    ("Sam Wright", "sam.wright@manufacturing.com"),
    ("Tina Scott", "tina.scott@pharma.com"),
]

# Realistic mock products (name, sku_prefix, base_price)
MOCK_PRODUCTS = [
    ("Wireless Bluetooth Headphones", "AUD-", 49.99),
    ("Organic Coffee Beans 1kg", "FOOD-", 24.99),
    ("USB-C Portable Charger 10000mAh", "ELEC-", 29.99),
    ("Yoga Mat 6mm Premium", "FIT-", 34.99),
    ("Stainless Steel Water Bottle", "LIFE-", 19.99),
    ("LED Desk Lamp with USB", "HOME-", 44.99),
    ("Running Shoes Men Size 10", "SHOE-", 89.99),
    ("Wireless Mouse Ergonomic", "OFF-", 39.99),
    ("Natural Face Moisturizer 50ml", "BEAU-", 27.99),
    ("Backpack Laptop 15 inch", "BAG-", 59.99),
    ("Mechanical Keyboard RGB", "TECH-", 79.99),
    ("Cotton Bed Sheet Set Queen", "HOME2-", 54.99),
    ("Protein Powder 2kg Vanilla", "NUT-", 42.99),
    ("Smart Watch Fitness Tracker", "WEAR-", 99.99),
    ("Noise Cancelling Earbuds", "AUD2-", 129.99),
]

# Realistic mock customers for MongoDB
MOCK_CUSTOMERS = [
    ("James Miller", "james.miller@customer.com", 1250.50),
    ("Emma Wilson", "emma.w@customer.com", 890.00),
    ("Liam Moore", "liam.moore@customer.com", 2100.75),
    ("Sophia Jackson", "sophia.j@customer.com", 450.25),
    ("Mason Martin", "mason.martin@customer.com", 3200.00),
    ("Isabella Thompson", "isabella.t@customer.com", 675.50),
    ("Ethan Garcia", "ethan.garcia@customer.com", 1890.00),
    ("Ava Martinez", "ava.martinez@customer.com", 540.75),
    ("William Robinson", "william.r@customer.com", 2750.25),
    ("Mia Clark", "mia.clark@customer.com", 980.00),
]


def seed_postgres(host: str = "localhost", port: int = PG_PORT) -> None:
    from sqlalchemy import text
    from sqlalchemy import create_engine

    url = f"postgresql://testuser:testpass@{host}:{port}/testdb"
    engine = create_engine(url)

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS test_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255) UNIQUE,
                age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS test_orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES test_users(id),
                amount DECIMAL(10,2),
                status VARCHAR(50)
            )
        """))
        conn.execute(text("DELETE FROM test_orders"))
        conn.execute(text("DELETE FROM test_users"))
        base_date = datetime(2024, 1, 1)
        for i, (name, email) in enumerate(MOCK_USERS, start=1):
            conn.execute(
                text("INSERT INTO test_users (id, name, email, age) VALUES (:id, :name, :email, :age)"),
                {"id": i, "name": name, "email": email, "age": 25 + (i % 45)},
            )
        # Realistic orders: varied amounts, statuses
        order_amounts = [99.99, 149.50, 24.99, 299.00, 45.00, 189.99, 12.50, 450.00, 67.99, 89.00]
        order_statuses = ["completed", "pending", "shipped", "completed", "cancelled", "pending", "completed", "refunded", "shipped", "completed"]
        for i in range(1, 11):
            conn.execute(
                text("INSERT INTO test_orders (id, user_id, amount, status) VALUES (:id, :uid, :amt, :status)"),
                {"id": i, "uid": (i % 5) + 1, "amt": order_amounts[i - 1], "status": order_statuses[i - 1]},
            )
    engine.dispose()
    print("PostgreSQL: seeded test_users and test_orders (realistic mock data)")


def seed_mysql(host: str = "localhost", port: int = MYSQL_PORT) -> None:
    import pymysql

    conn = pymysql.connect(
        host=host,
        port=port,
        user="testuser",
        password="testpass",
        database="testdb",
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255),
                    sku VARCHAR(100),
                    price DECIMAL(10,2),
                    stock INT
                )
            """)
            cur.execute("DELETE FROM test_products")
            for i, (name, sku_prefix, base_price) in enumerate(MOCK_PRODUCTS, start=1):
                sku = f"{sku_prefix}{i:04d}"
                price = round(base_price * (0.95 + (i % 10) / 50), 2)
                stock = 15 + (i % 85)
                cur.execute(
                    "INSERT INTO test_products (name, sku, price, stock) VALUES (%s, %s, %s, %s)",
                    (name, sku, price, stock),
                )
            conn.commit()
        print("MySQL: seeded test_products (realistic mock data)")
    finally:
        conn.close()


def seed_mongodb(host: str = "localhost", port: int = MONGO_PORT) -> None:
    from bson.objectid import ObjectId
    from pymongo import MongoClient

    # Use root credentials; mongo image uses MONGO_INITDB_ROOT_*
    uri = f"mongodb://testuser:testpass@{host}:{port}/?authSource=admin"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client["testdb"]  # Create testdb if not exists
    col = db["test_customers"]
    col.delete_many({})
    # Realistic customer documents - ObjectId _id required for tap-mongodb incremental sync
    addresses = [
        {"street": "123 Main St", "city": "San Francisco", "state": "CA", "zip": "94102"},
        {"street": "456 Oak Ave", "city": "New York", "state": "NY", "zip": "10001"},
        {"street": "789 Pine Rd", "city": "Austin", "state": "TX", "zip": "78701"},
        {"street": "321 Elm Blvd", "city": "Seattle", "state": "WA", "zip": "98101"},
        {"street": "654 Maple Dr", "city": "Denver", "state": "CO", "zip": "80202"},
        {"street": "987 Cedar Ln", "city": "Boston", "state": "MA", "zip": "02101"},
        {"street": "147 Birch St", "city": "Chicago", "state": "IL", "zip": "60601"},
        {"street": "258 Walnut Ave", "city": "Miami", "state": "FL", "zip": "33101"},
        {"street": "369 Spruce Way", "city": "Portland", "state": "OR", "zip": "97201"},
        {"street": "741 Ash Ct", "city": "Phoenix", "state": "AZ", "zip": "85001"},
    ]
    docs = []
    for i, (name, email, balance) in enumerate(MOCK_CUSTOMERS):
        docs.append({
            "_id": ObjectId(),
            "name": name,
            "email": email,
            "balance": balance,
            "address": addresses[i],
            "phone": f"+1-555-{100 + i:03d}-{2000 + i:04d}",
            "tier": "premium" if balance > 1500 else ("standard" if balance > 800 else "basic"),
            "created_at": (datetime.now(timezone.utc) - timedelta(days=30 - i)).isoformat(),
        })
    col.insert_many(docs)
    client.close()
    print("MongoDB: seeded test_customers (realistic mock data, ObjectId _id for tap-mongodb)")


def main() -> None:
    host = os.getenv("DB_HOST", "localhost")
    print(f"Seeding test data (host={host}, pg={PG_PORT}, mysql={MYSQL_PORT}, mongo={MONGO_PORT})")

    try:
        seed_postgres(host, PG_PORT)
    except Exception as e:
        print(f"PostgreSQL seed failed: {e}")
        sys.exit(1)

    try:
        seed_mysql(host, MYSQL_PORT)
    except Exception as e:
        print(f"MySQL seed failed: {e}")
        sys.exit(1)

    try:
        seed_mongodb(host, MONGO_PORT)
    except Exception as e:
        print(f"MongoDB seed failed: {e}")
        sys.exit(1)

    print("All seeds completed.")


if __name__ == "__main__":
    main()
