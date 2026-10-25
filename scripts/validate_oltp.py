from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"

def inspect_table(filename):
    path = DATA_DIR / filename

    print(f"\n{'=' * 60}")
    print(f"Table: {filename}")
    print("=" * 60)

    df = pd.read_csv(path, nrows=5)

    print("\nColumns:")
    print(df.columns.tolist())

    print("\nFirst 5 rows:")
    print(df.to_string(index=False))


tables = [
    "customers.csv",
    "products.csv",
    "orders.csv",
    "order_items.csv",
    "inventory.csv",
]

for table in tables:
    inspect_table(table)