#!/usr/bin/env python3
"""
Generate synthetic OLTP tables from one or more e-commerce clickstream CSV files.

Input:
  event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session

Outputs in data/processed/:
  customers.csv, products.csv, orders.csv, order_items.csv, inventory.csv

Modes:
  replace: rebuild outputs from all supplied source files.
  upsert: load existing output CSVs, then insert/update records represented by
          the supplied source files. For correct cross-month session aggregation,
          pass ALL source months each time (e.g. October and November together).

The SQLite file is a local work database, not a replacement for the CSV outputs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import random
import sqlite3
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd
from faker import Faker


COUNTRIES = [
    "Egypt", "Saudi Arabia", "UAE", "Jordan", "Kuwait", "Qatar", "Germany",
    "France", "United Kingdom", "United States", "Canada", "Australia",
    "New Zealand", "Japan", "South Korea", "China", "India", "Singapore",
    "Malaysia", "Indonesia", "Thailand", "Vietnam", "Philippines", "Turkey",
    "Italy", "Spain", "Portugal", "Netherlands", "Belgium", "Switzerland",
    "Austria", "Sweden", "Norway", "Denmark", "Finland", "Ireland", "Poland",
    "Czech Republic", "Greece", "Brazil", "Argentina", "Mexico", "Chile",
    "South Africa", "Nigeria", "Morocco", "Oman", "Bahrain", "Lebanon",
    "Tunisia",
]

ORDER_STATUSES = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Refunded"]
ORDER_STATUS_WEIGHTS = [5, 8, 12, 70, 3, 2]

INVENTORY_ORIGINS = [
    "Warehouse", "Store", "Supplier", "Dropship", "Manufacturer",
    "Distributor", "Returns", "Transfer", "Import", "Local Vendor",
]
INVENTORY_ORIGIN_WEIGHTS = [35, 20, 12, 8, 7, 6, 2, 3, 4, 3]

REQUIRED_COLUMNS = [
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session",
]

OUTPUT_HEADERS = {
    "customers": ["CustomerID", "FirstName", "LastName", "Email", "Country", "RegistrationDate"],
    "products": ["product_id", "category_id", "category_code", "brand", "price", "cost"],
    "orders": ["order_id", "customer_id", "order_timestamp", "order_total", "order_status"],
    "order_items": [
        "order_item_id", "order_id", "product_id", "quantity", "unit_price",
        "line_total", "first_purchase_time",
    ],
    "inventory": ["product_id", "stock_quantity", "reorder_level", "inventory_origin"],
}

OUTPUT_FILES = {
    name: f"{name}.csv"
    for name in ("customers", "products", "orders", "order_items", "inventory")
}


def order_key(user_id: str, session_id: str) -> str:
    """Stable order key; source user IDs are numeric and separated from session IDs."""
    return f"{user_id}_{session_id}"


def normalize_timestamp(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA foreign_keys=OFF;

    CREATE TABLE IF NOT EXISTS customer_stage (
        user_id TEXT PRIMARY KEY,
        registration_date TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS product_stage (
        product_id TEXT PRIMARY KEY,
        category_id TEXT, category_id_ts TEXT,
        category_code TEXT, category_code_ts TEXT,
        brand TEXT, brand_ts TEXT,
        price REAL, price_ts TEXT
    );

    CREATE TABLE IF NOT EXISTS order_stage (
        order_id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        order_timestamp TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS item_stage (
        order_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        product_id TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price REAL,
        first_purchase_time TEXT NOT NULL,
        last_purchase_time TEXT NOT NULL,
        PRIMARY KEY (order_id, product_id)
    );

    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        country TEXT NOT NULL,
        registration_date TEXT
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id TEXT PRIMARY KEY,
        category_id TEXT,
        category_code TEXT,
        brand TEXT,
        price REAL,
        cost REAL
    );

    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        order_timestamp TEXT NOT NULL,
        order_total REAL NOT NULL DEFAULT 0,
        order_status TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT NOT NULL,
        product_id TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price REAL,
        line_total REAL,
        first_purchase_time TEXT NOT NULL,
        UNIQUE(order_id, product_id)
    );

    CREATE TABLE IF NOT EXISTS inventory (
        product_id TEXT PRIMARY KEY,
        stock_quantity INTEGER NOT NULL,
        reorder_level INTEGER NOT NULL,
        inventory_origin TEXT NOT NULL
    );
    """)


def clear_stage_tables(conn: sqlite3.Connection) -> None:
    for table in ("customer_stage", "product_stage", "order_stage", "item_stage"):
        conn.execute(f"DELETE FROM {table}")


def clear_output_tables(conn: sqlite3.Connection) -> None:
    for table in ("order_items", "orders", "inventory", "products", "customers"):
        conn.execute(f"DELETE FROM {table}")
    # Keep item IDs compact in replace mode.
    conn.execute("DELETE FROM sqlite_sequence WHERE name='order_items'")


def read_existing_csv(conn: sqlite3.Connection, output_dir: Path) -> None:
    """Load existing output CSVs into SQLite without holding them in memory."""
    specs = {
        "customers": (
            "customers.csv",
            "INSERT OR REPLACE INTO customers VALUES (?, ?, ?, ?, ?, ?)",
            lambda r: (
                r.get("CustomerID", ""), r.get("FirstName", ""), r.get("LastName", ""),
                r.get("Email", ""), r.get("Country", ""), r.get("RegistrationDate") or None,
            ),
        ),
        "products": (
            "products.csv",
            "INSERT OR REPLACE INTO products VALUES (?, ?, ?, ?, ?, ?)",
            lambda r: (
                r.get("product_id", ""), r.get("category_id") or None,
                r.get("category_code") or None, r.get("brand") or None,
                as_float(r.get("price")), as_float(r.get("cost")),
            ),
        ),
        "orders": (
            "orders.csv",
            "INSERT OR REPLACE INTO orders VALUES (?, ?, ?, ?, ?)",
            lambda r: (
                r.get("order_id", ""), r.get("customer_id", ""),
                r.get("order_timestamp", ""), as_float(r.get("order_total")) or 0.0,
                r.get("order_status", "Pending"),
            ),
        ),
        "order_items": (
            "order_items.csv",
            """INSERT OR REPLACE INTO order_items
               (order_item_id, order_id, product_id, quantity, unit_price, line_total, first_purchase_time)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            lambda r: (
                as_int(r.get("order_item_id")), r.get("order_id", ""), r.get("product_id", ""),
                as_int(r.get("quantity")) or 1, as_float(r.get("unit_price")),
                as_float(r.get("line_total")), r.get("first_purchase_time", ""),
            ),
        ),
        "inventory": (
            "inventory.csv",
            "INSERT OR REPLACE INTO inventory VALUES (?, ?, ?, ?)",
            lambda r: (
                r.get("product_id", ""), as_int(r.get("stock_quantity")) or 0,
                as_int(r.get("reorder_level")) or 10, r.get("inventory_origin", "Warehouse"),
            ),
        ),
    }

    for table, (filename, insert_sql, convert) in specs.items():
        path = output_dir / filename
        if not path.exists():
            continue
        print(f"Loading existing {filename} for upsert...")
        batch = []
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                values = convert(row)
                # Skip malformed rows instead of aborting the entire update.
                if not values or any(v == "" for v in values[:1]):
                    continue
                batch.append(values)
                if len(batch) >= 10_000:
                    conn.executemany(insert_sql, batch)
                    conn.commit()
                    batch.clear()
        if batch:
            conn.executemany(insert_sql, batch)
            conn.commit()


def as_float(value):
    if value is None or value == "":
        return None
    try:
        result = float(value)
        return result if pd.notna(result) else None
    except (TypeError, ValueError):
        return None


def as_int(value):
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def upsert_customer_dates(conn: sqlite3.Connection, chunk: pd.DataFrame) -> None:
    data = chunk.dropna(subset=["user_id", "event_time"])
    if data.empty:
        return
    grouped = data.groupby("user_id", sort=False)["event_time"].min()
    rows = [(str(uid), normalize_timestamp(ts)) for uid, ts in grouped.items()]
    conn.executemany("""
        INSERT INTO customer_stage(user_id, registration_date) VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
          registration_date = MIN(customer_stage.registration_date, excluded.registration_date)
    """, rows)


def latest_field_values(chunk: pd.DataFrame, field: str) -> dict[str, tuple[object, str]]:
    valid = chunk.dropna(subset=["product_id", field, "event_time"])
    if valid.empty:
        return {}
    valid = valid.sort_values("event_time")
    last = valid.drop_duplicates("product_id", keep="last")
    return {
        str(row.product_id): (getattr(row, field), normalize_timestamp(row.event_time))
        for row in last[["product_id", field, "event_time"]].itertuples(index=False, name="Row")
    }


def upsert_product_metadata(conn: sqlite3.Connection, chunk: pd.DataFrame) -> None:
    product_ids = chunk["product_id"].dropna().astype(str).unique().tolist()
    if not product_ids:
        return
    fields = ["category_id", "category_code", "brand", "price"]
    latest = {field: latest_field_values(chunk, field) for field in fields}
    rows = []
    for pid in product_ids:
        row = [pid]
        for field in fields:
            value_ts = latest[field].get(pid)
            if value_ts:
                value, ts = value_ts
                if field == "price":
                    value = as_float(value)
                else:
                    value = str(value) if pd.notna(value) else None
                row.extend([value, ts])
            else:
                row.extend([None, None])
        rows.append(tuple(row))
    conn.executemany("""
        INSERT INTO product_stage(
          product_id, category_id, category_id_ts, category_code, category_code_ts,
          brand, brand_ts, price, price_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
          category_id = CASE WHEN excluded.category_id_ts IS NOT NULL
              AND (product_stage.category_id_ts IS NULL OR excluded.category_id_ts > product_stage.category_id_ts)
              THEN excluded.category_id ELSE product_stage.category_id END,
          category_id_ts = CASE WHEN excluded.category_id_ts IS NOT NULL
              AND (product_stage.category_id_ts IS NULL OR excluded.category_id_ts > product_stage.category_id_ts)
              THEN excluded.category_id_ts ELSE product_stage.category_id_ts END,
          category_code = CASE WHEN excluded.category_code_ts IS NOT NULL
              AND (product_stage.category_code_ts IS NULL OR excluded.category_code_ts > product_stage.category_code_ts)
              THEN excluded.category_code ELSE product_stage.category_code END,
          category_code_ts = CASE WHEN excluded.category_code_ts IS NOT NULL
              AND (product_stage.category_code_ts IS NULL OR excluded.category_code_ts > product_stage.category_code_ts)
              THEN excluded.category_code_ts ELSE product_stage.category_code_ts END,
          brand = CASE WHEN excluded.brand_ts IS NOT NULL
              AND (product_stage.brand_ts IS NULL OR excluded.brand_ts > product_stage.brand_ts)
              THEN excluded.brand ELSE product_stage.brand END,
          brand_ts = CASE WHEN excluded.brand_ts IS NOT NULL
              AND (product_stage.brand_ts IS NULL OR excluded.brand_ts > product_stage.brand_ts)
              THEN excluded.brand_ts ELSE product_stage.brand_ts END,
          price = CASE WHEN excluded.price_ts IS NOT NULL
              AND (product_stage.price_ts IS NULL OR excluded.price_ts > product_stage.price_ts)
              THEN excluded.price ELSE product_stage.price END,
          price_ts = CASE WHEN excluded.price_ts IS NOT NULL
              AND (product_stage.price_ts IS NULL OR excluded.price_ts > product_stage.price_ts)
              THEN excluded.price_ts ELSE product_stage.price_ts END
    """, rows)


def upsert_purchase_data(conn: sqlite3.Connection, chunk: pd.DataFrame) -> tuple[int, int]:
    purchases = chunk.loc[chunk["event_type"].eq("purchase")].copy()
    if purchases.empty:
        return 0, 0

    purchases = purchases.dropna(subset=["user_id", "user_session", "product_id", "event_time"])
    if purchases.empty:
        return 0, 0

    purchases["user_id"] = purchases["user_id"].astype(str)
    purchases["user_session"] = purchases["user_session"].astype(str)
    purchases["product_id"] = purchases["product_id"].astype(str)
    purchases = purchases.sort_values("event_time")

    # One order per (user, session), using the earliest purchase timestamp.
    grouped_orders = purchases.groupby(["user_id", "user_session"], sort=False)["event_time"].min()
    order_rows = [
        (order_key(str(uid), str(sess)), str(uid), normalize_timestamp(ts))
        for (uid, sess), ts in grouped_orders.items()
    ]
    conn.executemany("""
        INSERT INTO order_stage(order_id, user_id, order_timestamp) VALUES (?, ?, ?)
        ON CONFLICT(order_id) DO UPDATE SET
          order_timestamp = MIN(order_stage.order_timestamp, excluded.order_timestamp)
    """, order_rows)

    # Each purchase event represents one unit. Repeated product/session events
    # become one line, with the latest purchase-event price as unit_price.
    grouped = purchases.groupby(["user_id", "user_session", "product_id"], sort=False)
    item_rows = []
    for (uid, session, pid), group in grouped:
        group = group.sort_values("event_time")
        nonnull_prices = group.dropna(subset=["price"])
        latest_price = as_float(nonnull_prices.iloc[-1]["price"]) if not nonnull_prices.empty else None
        first_time = normalize_timestamp(group["event_time"].min())
        last_time = normalize_timestamp(group["event_time"].max())
        item_rows.append((
            order_key(str(uid), str(session)), str(uid), str(session), str(pid),
            int(len(group)), latest_price, first_time, last_time,
        ))

    conn.executemany("""
        INSERT INTO item_stage(
          order_id, user_id, session_id, product_id, quantity, unit_price,
          first_purchase_time, last_purchase_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(order_id, product_id) DO UPDATE SET
          quantity = item_stage.quantity + excluded.quantity,
          unit_price = CASE
            WHEN excluded.last_purchase_time >= item_stage.last_purchase_time
            THEN excluded.unit_price ELSE item_stage.unit_price END,
          first_purchase_time = MIN(item_stage.first_purchase_time, excluded.first_purchase_time),
          last_purchase_time = MAX(item_stage.last_purchase_time, excluded.last_purchase_time)
    """, item_rows)
    return len(purchases), len(item_rows)


def process_inputs(conn: sqlite3.Connection, input_files: list[Path], chunksize: int) -> None:
    total_rows = 0
    total_purchase_events = 0
    for file_path in input_files:
        if not file_path.exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")
        print(f"\nProcessing {file_path}...")
        file_rows = 0
        for chunk in pd.read_csv(
            file_path,
            usecols=lambda c: c in REQUIRED_COLUMNS,
            dtype={
                "event_type": "string", "product_id": "string", "category_id": "string",
                "category_code": "string", "brand": "string", "user_id": "string",
                "user_session": "string",
            },
            chunksize=chunksize,
            low_memory=False,
        ):
            missing = set(REQUIRED_COLUMNS) - set(chunk.columns)
            if missing:
                raise ValueError(f"{file_path} is missing required columns: {sorted(missing)}")

            chunk["event_time"] = pd.to_datetime(chunk["event_time"], utc=True, errors="coerce")
            chunk["price"] = pd.to_numeric(chunk["price"], errors="coerce")
            chunk = chunk.dropna(subset=["event_time"])
            if chunk.empty:
                continue

            upsert_customer_dates(conn, chunk)
            upsert_product_metadata(conn, chunk)
            purchase_count, _ = upsert_purchase_data(conn, chunk)

            file_rows += len(chunk)
            total_purchase_events += purchase_count
            conn.commit()
            print(
                f"\r  rows read: {file_rows:,} | purchase events: {total_purchase_events:,}",
                end="", flush=True,
            )
        total_rows += file_rows
        print()
    print(f"\nTotal valid rows processed: {total_rows:,}")
    print(f"Purchase events processed: {total_purchase_events:,}")


def stable_random(seed: int, key: str) -> random.Random:
    digest = hashlib.sha256(f"{seed}:{key}".encode("utf-8")).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def make_unique_email(conn: sqlite3.Connection, first: str, last: str, user_id: str) -> str:
    digits = "".join(ch for ch in user_id if ch.isdigit())
    suffix = digits[:4] or "0000"
    base = f"{first.lower()}.{last.lower()}{suffix}@gmail.com"
    candidate = base
    counter = 1
    while conn.execute("SELECT 1 FROM customers WHERE email = ?", (candidate,)).fetchone():
        stem = base[:-10]  # remove '@gmail.com'
        candidate = f"{stem}{counter}@gmail.com"
        counter += 1
    return candidate


def merge_staging_into_outputs(conn: sqlite3.Connection, seed: int) -> None:
    fake = Faker()
    fake.seed_instance(seed)
    rng = random.Random(seed)

    print("\nMerging customers...")
    customer_rows = conn.execute("""
        SELECT s.user_id, s.registration_date
        FROM customer_stage s
        ORDER BY s.user_id
    """)
    inserted = 0
    for user_id, reg_date in customer_rows:
        existing = conn.execute(
            "SELECT 1 FROM customers WHERE customer_id = ?", (user_id,)
        ).fetchone()
        if existing:
            conn.execute("""
                UPDATE customers SET
                  registration_date = CASE
                    WHEN registration_date IS NULL THEN ?
                    WHEN ? IS NULL THEN registration_date
                    ELSE MIN(registration_date, ?) END
                WHERE customer_id = ?
            """, (reg_date, reg_date, reg_date, user_id))
            continue
        first, last = fake.first_name(), fake.last_name()
        email = make_unique_email(conn, first, last, str(user_id))
        country = rng.choice(COUNTRIES)
        conn.execute("""
            INSERT INTO customers(customer_id, first_name, last_name, email, country, registration_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, first, last, email, country, reg_date))
        inserted += 1
        if inserted % 25_000 == 0:
            conn.commit()
            print(f"\r  new customers generated: {inserted:,}", end="", flush=True)
    conn.commit()
    print(f"\n  new customers generated: {inserted:,}")

    print("Merging products...")
    product_rows = conn.execute("""
        SELECT product_id, category_id, category_code, brand, price
        FROM product_stage ORDER BY product_id
    """)
    new_products = 0
    for pid, category_id, category_code, brand, price in product_rows:
        existing = conn.execute("SELECT cost FROM products WHERE product_id = ?", (pid,)).fetchone()
        if existing:
            # Update current catalog metadata, but preserve previously generated cost.
            conn.execute("""
                UPDATE products SET
                  category_id = COALESCE(?, category_id),
                  category_code = COALESCE(?, category_code),
                  brand = COALESCE(?, brand),
                  price = COALESCE(?, price)
                WHERE product_id = ?
            """, (category_id, category_code, brand, price, pid))
        else:
            if price is not None and (price < 0 or price != price or price == float("inf")):
                raise ValueError(f"Invalid product price for product_id={pid}: {price}")
            local_rng = stable_random(seed, f"cost:{pid}")
            cost = round(float(price) * local_rng.uniform(0.50, 0.85), 2) if price is not None else None
            conn.execute("""
                INSERT INTO products(product_id, category_id, category_code, brand, price, cost)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (pid, category_id, category_code, brand, price, cost))
            new_products += 1
        if new_products and new_products % 25_000 == 0:
            conn.commit()
            print(f"\r  new products inserted: {new_products:,}", end="", flush=True)
    conn.commit()
    print(f"\n  new products inserted: {new_products:,}")

    print("Merging orders and order items...")
    for order_id, user_id, order_timestamp in conn.execute(
        "SELECT order_id, user_id, order_timestamp FROM order_stage"
    ):
        conn.execute("""
            INSERT INTO orders(order_id, customer_id, order_timestamp, order_total, order_status)
            VALUES (?, ?, ?, 0, ?)
            ON CONFLICT(order_id) DO UPDATE SET
              customer_id = excluded.customer_id,
              order_timestamp = MIN(orders.order_timestamp, excluded.order_timestamp)
        """, (
            order_id, user_id, order_timestamp,
            rng.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0],
        ))

    for row in conn.execute("""
        SELECT order_id, product_id, quantity, unit_price, first_purchase_time
        FROM item_stage
    """):
        order_id, product_id, quantity, unit_price, first_time = row
        line_total = round(quantity * unit_price, 2) if unit_price is not None else None
        conn.execute("""
            INSERT INTO order_items(order_id, product_id, quantity, unit_price, line_total, first_purchase_time)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id, product_id) DO UPDATE SET
              quantity = excluded.quantity,
              unit_price = excluded.unit_price,
              line_total = excluded.line_total,
              first_purchase_time = MIN(order_items.first_purchase_time, excluded.first_purchase_time)
        """, (order_id, product_id, quantity, unit_price, line_total, first_time))
    conn.commit()

    # Keep an existing order status when upserting. New orders get a weighted status.
    # Recalculate all totals from order_items, including orders updated in this run.
    conn.execute("""
        UPDATE orders
        SET order_total = COALESCE((
            SELECT ROUND(SUM(oi.line_total), 2)
            FROM order_items oi WHERE oi.order_id = orders.order_id
        ), 0)
    """)
    conn.commit()

    print("Merging inventory...")
    for (pid,) in conn.execute("SELECT product_id FROM products"):
        exists = conn.execute("SELECT 1 FROM inventory WHERE product_id = ?", (pid,)).fetchone()
        if exists:
            continue
        local_rng = stable_random(seed, f"inventory:{pid}")
        stock_quantity = local_rng.randint(0, 500)
        reorder_level = local_rng.randint(10, 100)
        origin = local_rng.choices(INVENTORY_ORIGINS, weights=INVENTORY_ORIGIN_WEIGHTS, k=1)[0]
        conn.execute("""
            INSERT INTO inventory(product_id, stock_quantity, reorder_level, inventory_origin)
            VALUES (?, ?, ?, ?)
        """, (pid, stock_quantity, reorder_level, origin))
    conn.commit()


def export_csv(conn: sqlite3.Connection, output_dir: Path) -> None:
    queries = {
        "customers": (
            "SELECT customer_id, first_name, last_name, email, country, registration_date FROM customers ORDER BY customer_id",
            OUTPUT_HEADERS["customers"],
        ),
        "products": (
            "SELECT product_id, category_id, category_code, brand, price, cost FROM products ORDER BY product_id",
            OUTPUT_HEADERS["products"],
        ),
        "orders": (
            "SELECT order_id, customer_id, order_timestamp, order_total, order_status FROM orders ORDER BY order_id",
            OUTPUT_HEADERS["orders"],
        ),
        "order_items": (
            """SELECT order_item_id, order_id, product_id, quantity, unit_price, line_total, first_purchase_time
               FROM order_items ORDER BY order_item_id""",
            OUTPUT_HEADERS["order_items"],
        ),
        "inventory": (
            "SELECT product_id, stock_quantity, reorder_level, inventory_origin FROM inventory ORDER BY product_id",
            OUTPUT_HEADERS["inventory"],
        ),
    }
    column_names = {
        "customers": ["customer_id", "first_name", "last_name", "email", "country", "registration_date"],
        "products": ["product_id", "category_id", "category_code", "brand", "price", "cost"],
        "orders": ["order_id", "customer_id", "order_timestamp", "order_total", "order_status"],
        "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_total", "first_purchase_time"],
        "inventory": ["product_id", "stock_quantity", "reorder_level", "inventory_origin"],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    for table, (query, headers) in queries.items():
        path = output_dir / OUTPUT_FILES[table]
        print(f"Writing {path}...")
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)
            for row in conn.execute(query):
                writer.writerow(row)
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {count:,} rows")


def validate(conn: sqlite3.Connection) -> None:
    print("\nValidation")
    checks = {
        "customers with missing email": "SELECT COUNT(*) FROM customers WHERE email IS NULL OR email = ''",
        "duplicate customer emails": "SELECT COUNT(*) FROM (SELECT email FROM customers GROUP BY email HAVING COUNT(*) > 1)",
        "products with invalid price": "SELECT COUNT(*) FROM products WHERE price < 0",
        "order items with non-positive quantity": "SELECT COUNT(*) FROM order_items WHERE quantity <= 0",
        "order items missing order": "SELECT COUNT(*) FROM order_items oi LEFT JOIN orders o ON oi.order_id=o.order_id WHERE o.order_id IS NULL",
        "order items missing product": "SELECT COUNT(*) FROM order_items oi LEFT JOIN products p ON oi.product_id=p.product_id WHERE p.product_id IS NULL",
        "inventory rows missing product": "SELECT COUNT(*) FROM inventory i LEFT JOIN products p ON i.product_id=p.product_id WHERE p.product_id IS NULL",
        "order totals differing from line totals": """
            SELECT COUNT(*) FROM orders o
            WHERE ABS(o.order_total - COALESCE((
              SELECT SUM(oi.line_total) FROM order_items oi WHERE oi.order_id=o.order_id
            ), 0)) > 0.011
        """,
        "products missing inventory": "SELECT COUNT(*) FROM products p LEFT JOIN inventory i ON p.product_id=i.product_id WHERE i.product_id IS NULL",
    }
    failed = False
    for label, sql in checks.items():
        count = conn.execute(sql).fetchone()[0]
        print(f"  {label}: {count:,}")
        if count:
            failed = True
    if failed:
        raise RuntimeError("One or more validation checks failed. Review the output above.")
    print("  All validation checks passed.")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", nargs="+", required=True, type=Path,
        help="One or more monthly source CSV files, e.g. data/2019-Oct.csv data/2019-Nov.csv",
    )
    parser.add_argument(
        "--mode", choices=("replace", "upsert"), default="replace",
        help="replace rebuilds outputs; upsert merges into existing output CSVs (default: replace)",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/processed"))
    parser.add_argument("--work-db", type=Path, default=Path("data/processed/oltp_build.sqlite"))
    parser.add_argument("--chunksize", type=int, default=200_000)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_files = [p.resolve() for p in args.input]
    if len(set(input_files)) != len(input_files):
        raise ValueError("The same input file was provided more than once.")
    if args.chunksize < 1:
        raise ValueError("--chunksize must be greater than zero.")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.work_db.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.work_db)
    try:
        create_tables(conn)
        clear_stage_tables(conn)
        clear_output_tables(conn)
        if args.mode == "upsert":
            read_existing_csv(conn, args.output_dir)
        conn.commit()

        print(f"Mode: {args.mode}")
        print(f"Input files: {len(input_files)}")
        print(f"Chunk size: {args.chunksize:,}")
        print(f"SQLite work database: {args.work_db}")

        process_inputs(conn, input_files, args.chunksize)
        merge_staging_into_outputs(conn, args.seed)
        validate(conn)
        export_csv(conn, args.output_dir)
        print("\nDone.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
