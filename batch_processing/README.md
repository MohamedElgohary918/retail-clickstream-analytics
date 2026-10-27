# Batch Processing — Retail Clickstream Analytics

## 1. Overview

This directory contains the batch-processing workflow for the **Real-Time Clickstream Ingestion & Omnichannel Data Warehouse** project.

The workflow transforms e-commerce clickstream events into five synthetic operational datasets, validates the generated data, and prepares it for manual ingestion into Snowflake.

The batch pipeline complements the streaming pipeline:

- **Batch pipeline:** Generates operational datasets from historical clickstream records and loads them into Snowflake.
- **Streaming pipeline:** Ingests clickstream events into Snowflake as they are produced by the streaming producer.

### Batch pipeline architecture

```text
E-commerce Clickstream CSV Files
              |
              v
    Explore Source Dataset
              |
              v
       Generate OLTP Data
              |
              v
       Validate OLTP Data
              |
              v
      Generated CSV Files
              |
              v
    Manual Upload to Snowflake
              |
              v
         Snowflake RAW
              |
              v
       dbt STAGING Models
              |
              v
         dbt MARTS
```

The local PostgreSQL database can be used to simulate the operational database. However, PostgreSQL is not required for the direct CSV-to-Snowflake ingestion path.

---

## 2. Source Dataset

The source is the [E-commerce Behavior Data from a Multi-Category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset.

It contains customer interactions with products, including views, cart additions, and purchases.

| Field | Description |
|---|---|
| `event_time` | Timestamp of the interaction |
| `event_type` | Event type, such as view, cart, or purchase |
| `product_id` | Product identifier |
| `category_id` | Product category identifier |
| `category_code` | Product category hierarchy, when available |
| `brand` | Product brand, when available |
| `price` | Price recorded for the event |
| `user_id` | Pseudonymous user identifier |
| `user_session` | Browsing session identifier |

The source files used by this project are stored in the project-level `data/` directory.

Example:

```text
data/
├── 2019-Oct.csv
└── 2019-Nov.csv
```

October 2019 can be used for an initial run. Additional monthly files can be included as needed.

---

## 3. Generated OLTP Datasets

The Python generator transforms the clickstream records into five synthetic operational datasets.

| Dataset | Source | Generation rule |
|---|---|---|
| `customers` | `user_id` | One customer per distinct source user |
| `products` | `product_id` | One product per distinct product identifier, with observed attributes |
| `orders` | Purchase events | One synthetic order per eligible customer session |
| `order_items` | Purchase events | Purchase lines associated with the generated orders |
| `inventory` | Generated values | Synthetic stock quantities for each product |

### Modeling assumptions

The generated operational records are a simulation, not a reconstruction of the store's original production database.

The following assumptions apply:

1. Each source `user_id` maps to one synthetic customer.
2. Each source `product_id` maps to one product.
3. Each eligible customer session containing purchase events maps to one synthetic order.
4. Each purchase event represents one unit for modeling purposes because the source does not provide an explicit purchase quantity.
5. Order totals are calculated from the recorded purchase-event prices and the quantity assumptions.
6. Payment status, shipping information, and inventory quantities are synthetic.
7. Sessions without purchase events do not generate orders.
8. Non-purchase events remain part of the original clickstream dataset and can support conversion-rate and cart-abandonment analysis.

**Important:** The generator's implementation should determine exactly how repeated purchase events are deduplicated or aggregated. The chosen policy should be documented and applied consistently.

---

## 4. Project Structure

```text
batch_processing/
├── 00_explore_stream_data.py
├── 01_generate_oltp.py
├── 02_validate_oltp.py
├── postgres/
│   ├── 03_Setup_postgress.md
│   └── 04_OLTP_Verification.sql
├── snowflake/
│   ├── 05_ Create_Database&Schemas.sql
│   └── 06_RAW_Tables.sql
└── README.md
```

### Script responsibilities

| File | Responsibility |
|---|---|
| `00_explore_stream_data.py` | Explore source columns, event types, data quality, and dataset characteristics |
| `01_generate_oltp.py` | Generate the five operational datasets |
| `02_validate_oltp.py` | Validate generated datasets and their relationships |
| `postgres/03_Setup_postgress.md` | Document local PostgreSQL setup |
| `postgres/04_OLTP_Verification.sql` | Verify the operational tables in PostgreSQL |
| `snowflake/05_ Create_Database&Schemas.sql` | Create the Snowflake database and schemas |
| `snowflake/06_RAW_Tables.sql` | Create the Snowflake RAW tables |

The scripts generate the CSV files in the project-level `data/processed/` directory.

---

## 5. Prerequisites

- Python 3
- The source clickstream CSV files
- Required Python packages
- A Snowflake account with permission to create and load tables
- Optional: Docker and PostgreSQL if you want to run the simulated OLTP database

Activate the existing project virtual environment from the project root:

```bash
cd ~/WorkSpace/retail-clickstream-analytics
source .venv/bin/activate
```

Install any missing dependencies required by the scripts.

For example:

```bash
pip install pandas numpy faker
```

Install additional packages only if the existing scripts require them. Kafka and dbt dependencies belong to their respective workflows and are not automatically required to generate the OLTP CSV files.

---

## 6. Execute the Batch Pipeline

Run the following commands from the project root:

```bash
cd ~/WorkSpace/retail-clickstream-analytics
```

### Step 1 — Explore the source dataset

```bash
python batch_processing/00_explore_stream_data.py
```

Review the output before generating the operational datasets.

### Step 2 — Generate the OLTP datasets

For an initial build using both October and November:

```bash
python batch_processing/01_generate_oltp.py \
  --input data/2019-Oct.csv data/2019-Nov.csv \
  --mode replace
```

This command assumes that the generator supports the specified arguments and accepts multiple input files. Confirm those options against the script's argument parser if they differ.

Expected outputs:

```text
data/processed/
├── customers.csv
├── products.csv
├── orders.csv
├── order_items.csv
├── inventory.csv
└── oltp_build.sqlite
```

The CSV files are the outputs intended for ingestion. The SQLite file is an intermediate work database used by the generator, if implemented that way.

### Step 3 — Validate the generated datasets

```bash
python batch_processing/02_validate_oltp.py
```

Review the validation results before uploading the data.

At minimum, validation should cover:

- Required columns and data types
- Missing values in required fields
- Duplicate primary keys
- Foreign-key relationships
- Order totals and quantities
- Row counts for all five datasets

Do not proceed to Snowflake until any unexpected validation failures have been investigated.

### Step 4 — Review generated file sizes

```bash
ls -lh data/processed/*.csv
```

Record the row counts and file sizes. These provide a baseline for verifying the Snowflake load.

To count data records without counting the header:

```bash
for f in data/processed/{customers,products,orders,order_items,inventory}.csv; do
    printf '%-20s ' "$(basename "$f")"
    awk 'END {print NR-1}' "$f"
done
```

This command assumes each CSV has one header row and no embedded newline characters that complicate line counting.

---

## 7. Optional: Verify the Local PostgreSQL Database

If you use PostgreSQL to simulate the operational database, follow:

1. `postgres/03_Setup_postgress.md`
2. `postgres/04_OLTP_Verification.sql`

This step allows you to demonstrate that the generated data can be stored in an operational relational database.

It is optional for the manual CSV upload workflow because the generated CSV files can be loaded directly into Snowflake.

---

## 8. Prepare Snowflake

Execute the SQL scripts in the following order:

1. `snowflake/05_ Create_Database&Schemas.sql`
2. `snowflake/06_RAW_Tables.sql`

The expected Snowflake layout is:

```text
RETAIL_CLICKSTREAM_DB
├── RAW
│   ├── RAW_CUSTOMERS
│   ├── RAW_PRODUCTS
│   ├── RAW_ORDERS
│   ├── RAW_ORDER_ITEMS
│   ├── RAW_INVENTORY
│   └── RAW_CLICKSTREAM_EVENTS
├── STAGING
└── MARTS
```

The five OLTP tables hold the generated operational datasets. `RAW_CLICKSTREAM_EVENTS` holds the event-level clickstream data ingested through the separate streaming pipeline.

Before loading the files, confirm that the column names, data types, and table definitions match the generated CSVs.

---

## 9. Manually Upload the CSV Files to Snowflake

The current workflow uses manual upload rather than an automated Airbyte or Python ingestion process.

Map each file to its corresponding table:

| Local CSV | Snowflake target |
|---|---|
| `customers.csv` | `RAW_CUSTOMERS` |
| `products.csv` | `RAW_PRODUCTS` |
| `orders.csv` | `RAW_ORDERS` |
| `order_items.csv` | `RAW_ORDER_ITEMS` |
| `inventory.csv` | `RAW_INVENTORY` |

For each dataset:

1. Open Snowflake Snowsight.
2. Select `RETAIL_CLICKSTREAM_DB` and the `RAW` schema.
3. Open the supported CSV loading workflow for the target table.
4. Select the corresponding CSV file.
5. Configure the file format and column mapping.
6. Confirm the preview and load settings.
7. Load the data.
8. Record the result and proceed to the next dataset.

### Large files

Check the size of every file before uploading.

If a file exceeds the upload limit of your chosen Snowsight workflow, split it into smaller chunks or use another supported Snowflake loading method.

If you split a CSV:

- Preserve the header consistently according to the loading method.
- Load every chunk into the same target table.
- Avoid loading the header as a data record.
- Track which chunks were successfully loaded.
- Avoid reloading a chunk accidentally, as that can introduce duplicates.

Manual upload is suitable for this small-scale project, but it is less convenient for repeated or large-scale ingestion.

---

## 10. Verify the Snowflake Load

After uploading the five files, verify that the target tables contain the expected data.

At minimum, compare the loaded row counts with the validated CSV row counts.

Example:

```sql
SELECT 'RAW_CUSTOMERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT
FROM RETAIL_CLICKSTREAM_DB.RAW.RAW_CUSTOMERS

UNION ALL

SELECT 'RAW_PRODUCTS', COUNT(*)
FROM RETAIL_CLICKSTREAM_DB.RAW.RAW_PRODUCTS

UNION ALL

SELECT 'RAW_ORDERS', COUNT(*)
FROM RETAIL_CLICKSTREAM_DB.RAW.RAW_ORDERS

UNION ALL

SELECT 'RAW_ORDER_ITEMS', COUNT(*)
FROM RETAIL_CLICKSTREAM_DB.RAW.RAW_ORDER_ITEMS

UNION ALL

SELECT 'RAW_INVENTORY', COUNT(*)
FROM RETAIL_CLICKSTREAM_DB.RAW.RAW_INVENTORY;
```

Compare these results with the CSV row counts recorded during validation.

Also verify:

- Required columns do not contain unexpected nulls.
- Primary-key values are unique.
- Order items reference existing orders and products.
- Orders reference existing customers.
- No CSV headers were loaded as data.
- Repeated uploads have not introduced duplicate records.

Create a dedicated verification SQL file in the `snowflake/` directory if you want to preserve these checks for future runs.

---

## 11. Downstream dbt Processing

After the RAW tables have been loaded and verified, use dbt to transform the data.

The intended downstream workflow is:

```text
Snowflake RAW
      |
      v
dbt STAGING
  - Rename columns
  - Cast data types
  - Standardize values
  - Apply data-quality tests
      |
      v
dbt MARTS
  - Sales analysis
  - Product/category analysis
  - Customer behavior analysis
```

Run the existing dbt project according to its configured models and targets.

Verify the staging models and tests before building the analytical marts.

---

## 12. Incremental Updates and Repeated Uploads

For an initial build, generate and load the complete datasets.

If you later add monthly source files, regenerate the datasets according to the behavior implemented in `01_generate_oltp.py`, then validate them again.

Do not assume that rerunning the generator in `upsert` mode automatically makes the Snowflake load incremental.

These are separate operations:

- **OLTP generation:** Determines how records are created or updated in the local outputs.
- **Snowflake ingestion:** Determines whether records are appended, replaced, merged, or deduplicated in Snowflake.

For repeated uploads, define a clear refresh strategy. A full refresh is simpler for a small demonstration, while incremental ingestion requires stable keys and a documented merge or deduplication procedure.

---

## 13. Limitations

- The operational datasets are synthetic models derived from clickstream events.
- Purchase quantity and order grouping are assumptions, not explicit facts from the source dataset.
- Inventory and payment-related values are synthetic.
- Manual upload requires human intervention and does not provide scheduled ingestion.
- Repeated uploads can introduce duplicates unless a refresh or merge strategy is used.
- Local PostgreSQL is optional when the CSV files are loaded directly into Snowflake.

---

## 14. Final Workflow Summary

1. Explore the clickstream source files.
2. Generate the five OLTP datasets.
3. Validate the CSV files and relationships.
4. Optionally load the datasets into local PostgreSQL.
5. Create the Snowflake database, schemas, and RAW tables.
6. Manually upload the five CSV files.
7. Verify the loaded row counts and data quality.
8. Run dbt staging models and tests.
9. Build the analytical marts.

This approach keeps the batch pipeline simple while preserving a clear separation between source exploration, OLTP generation, data validation, ingestion, and analytical transformation.