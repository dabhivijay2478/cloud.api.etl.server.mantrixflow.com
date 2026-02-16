#!/usr/bin/env python3
"""
Seed dummy test data into PostgreSQL, MySQL, and MongoDB.
Run after docker-compose.test.yml databases are up.
Uses ports: Postgres 15432, MySQL 13306, MongoDB 27018
"""
from __future__ import annotations

import os
import sys
from urllib.parse import quote_plus

# Add parent so we can import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Default to test ports (override via env for different setups)
PG_PORT = int(os.getenv("PG_TEST_PORT", "15432"))
MYSQL_PORT = int(os.getenv("MYSQL_TEST_PORT", "13306"))
MONGO_PORT = int(os.getenv("MONGO_TEST_PORT", "27018"))


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
        for i in range(1, 21):
            conn.execute(
                text("INSERT INTO test_users (id, name, email, age) VALUES (:id, :name, :email, :age)"),
                {"id": i, "name": f"User {i}", "email": f"user{i}@test.com", "age": 20 + (i % 50)},
            )
        for i in range(1, 11):
            conn.execute(
                text("INSERT INTO test_orders (id, user_id, amount, status) VALUES (:id, :uid, :amt, :status)"),
                {"id": i, "uid": (i % 5) + 1, "amt": 10.5 * i, "status": "pending" if i % 2 else "completed"},
            )
    engine.dispose()
    print("PostgreSQL: seeded test_users and test_orders")


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
            for i in range(1, 16):
                cur.execute(
                    "INSERT INTO test_products (name, sku, price, stock) VALUES (%s, %s, %s, %s)",
                    (f"Product {i}", f"SKU-{i:04d}", 9.99 * i, 10 + i),
                )
            conn.commit()
        print("MySQL: seeded test_products")
    finally:
        conn.close()


def seed_mongodb(host: str = "localhost", port: int = MONGO_PORT) -> None:
    from pymongo import MongoClient

    # Use root credentials; mongo image uses MONGO_INITDB_ROOT_*
    uri = f"mongodb://testuser:testpass@{host}:{port}/?authSource=admin"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client["testdb"]  # Create testdb if not exists
    col = db["test_customers"]
    col.delete_many({})
    docs = [
        {"_id": f"cust_{i}", "name": f"Customer {i}", "email": f"c{i}@example.com", "balance": 100.0 * i}
        for i in range(1, 11)
    ]
    col.insert_many(docs)
    client.close()
    print("MongoDB: seeded test_customers")


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
