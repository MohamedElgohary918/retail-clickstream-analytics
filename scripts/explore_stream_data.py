from pathlib import Path

import pandas as pd

# --------------------------------------------------
# Configuration
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
FILE_PATH = BASE_DIR / "data" / "2019-Oct.csv"

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 120)

# --------------------------------------------------
# 1. Check the dataset
# --------------------------------------------------

if not FILE_PATH.exists():
    raise FileNotFoundError(f"Dataset not found: {FILE_PATH}")

file_size_gb = FILE_PATH.stat().st_size / (1024**3)

print("=" * 60)
print("DATASET INFORMATION")
print("=" * 60)
print(f"File: {FILE_PATH}")
print(f"File size: {file_size_gb:.2f} GB")

# --------------------------------------------------
# 2. Load the complete dataset
# --------------------------------------------------

print("\nLoading dataset...")

df = pd.read_csv(
    FILE_PATH,
    parse_dates=["event_time"]
)

print("Dataset loaded successfully.")

# --------------------------------------------------
# 3. Dataset dimensions
# --------------------------------------------------

print("\n" + "=" * 60)
print("DATASET DIMENSIONS")
print("=" * 60)

print(f"Total rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")

print("\nColumns:")
print(df.columns.tolist())

print("\nData types:")
print(df.dtypes)

# --------------------------------------------------
# 4. Missing values
# --------------------------------------------------

print("\n" + "=" * 60)
print("MISSING VALUES")
print("=" * 60)

missing_summary = pd.DataFrame({
    "missing_count": df.isnull().sum(),
    "missing_percentage": (
        df.isnull().mean() * 100
    ).round(2),
})

print(
    missing_summary
    .sort_values("missing_percentage", ascending=False)
    .to_string()
)

# --------------------------------------------------
# 5. Event distribution
# --------------------------------------------------

print("\n" + "=" * 60)
print("EVENT DISTRIBUTION")
print("=" * 60)

event_counts = df["event_type"].value_counts(dropna=False)

event_summary = pd.DataFrame({
    "event_count": event_counts,
    "percentage": (
        event_counts / len(df) * 100
    ).round(2),
})

print(event_summary.to_string())

# --------------------------------------------------
# 6. Unique entities
# --------------------------------------------------

print("\n" + "=" * 60)
print("UNIQUE ENTITIES")
print("=" * 60)

print(f"Unique users: {df['user_id'].nunique():,}")
print(f"Unique sessions: {df['user_session'].nunique():,}")
print(f"Unique products: {df['product_id'].nunique():,}")
print(f"Unique categories: {df['category_id'].nunique():,}")

# --------------------------------------------------
# 7. Session statistics
# --------------------------------------------------

print("\n" + "=" * 60)
print("SESSION STATISTICS")
print("=" * 60)

events_per_session = df.groupby("user_session").size()

print(f"Total sessions: {len(events_per_session):,}")
print(f"Average events per session: {events_per_session.mean():.2f}")
print(f"Median events per session: {events_per_session.median():.0f}")
print(f"Maximum events in a session: {events_per_session.max():,}")

print("\nEvents per session distribution:")
print(events_per_session.describe().to_string())

# --------------------------------------------------
# 8. Purchase analysis
# --------------------------------------------------

print("\n" + "=" * 60)
print("PURCHASE ANALYSIS")
print("=" * 60)

purchases = df[df["event_type"] == "purchase"]

purchases_per_session = (
    purchases.groupby("user_session")
    .size()
)

print(f"Total purchase events: {len(purchases):,}")
print(f"Users with purchase events: {purchases['user_id'].nunique():,}")
print(f"Sessions with purchase events: {purchases['user_session'].nunique():,}")
print(f"Unique products purchased: {purchases['product_id'].nunique():,}")

print("\nPurchase events per purchasing session:")
print(purchases_per_session.describe().to_string())

print(
    "\nSessions with multiple purchase events: "
    f"{(purchases_per_session > 1).sum():,}"
)

# --------------------------------------------------
# 9. Event time range
# --------------------------------------------------

print("\n" + "=" * 60)
print("EVENT TIME RANGE")
print("=" * 60)

print(f"Earliest event: {df['event_time'].min()}")
print(f"Latest event: {df['event_time'].max()}")

print("\nExploration completed.")
