# Retail Clickstream Analytics — dbt Staging & Data Quality

This folder contains the **data preparation and quality assurance** portion of the Retail Clickstream Analytics project. It uses **dbt Core with Snowflake** to transform six source datasets into validated staging views for the downstream Data Warehouse and Analytics teams.

> **Scope:** source-level cleaning, standardization, quality flags, tests, and handoff. This project does **not** implement fact/dimension models, business KPIs, or orchestration.

## Architecture

```text
PostgreSQL OLTP / Kaggle-based data ─┐
                                     ├──> Snowflake RAW ──> dbt STAGING views ──> Warehouse models / Gold ──> Insights
Kafka clickstream replay ────────────┘
```

- **Snowflake database:** `RETAIL_CLICKSTREAM_DB`
- **Source schema:** `RAW`
- **Staging schema:** `STAGING`
- **Warehouse (current environment):** `RETAIL_WH`
- **Role (current environment):** `DBT_SILVER_ROLE`
- **Materialization:** Snowflake **views**; this layer does not persist duplicate copies of source rows.

The staging views query the underlying RAW tables when read. New rows already committed to RAW therefore appear in staging without another `dbt run`, as long as the source schema remains compatible. This does **not** mean that dbt tests run automatically, or that Kafka ingestion is exactly-once.

## Sources and Models

| Snowflake RAW source | dbt model | Record grain | Primary technical work |
|---|---|---|---|
| `RAW_CUSTOMERS` | `stg_customers` | One customer | Trim text; lowercase/trim email |
| `RAW_PRODUCTS` | `stg_products` | One product | Normalize descriptive text; retain missing category/brand; quality flags for missing or invalid attributes |
| `RAW_ORDERS` | `stg_orders` | One order | Standardize status; identify invalid totals and missing timestamps |
| `RAW_ORDER_ITEMS` | `stg_order_items` | One order item | Validate quantity, unit price, line total, and arithmetic consistency |
| `RAW_INVENTORY` | `stg_inventory` | One product inventory record | Flag invalid stock/reorder values; retain valid zero-stock records |
| `RAW_CLICKSTREAM_EVENTS` | `stg_clickstream_events` | One source event row | Standardize event types; preserve original/replay times; composite user/session key; quality flags |

All six models preserve the **source row grain**. Cross-table joins, business aggregations, star-schema design, and KPI definitions are intentionally left to the downstream modeling team.

### Data handling policies

- **Missing category/brand:** retain source `NULL` values and expose display labels or missing-value flags where implemented. Missing descriptive fields do not trigger row deletion.
- **Zero values:** keep plausible values (such as out-of-stock inventory and zero-priced products) and flag them for interpretation; distinguish them from invalid negative values.
- **Clickstream duplicates:** do not drop events just because payload fields match. RAW currently lacks a stable event ID or Kafka topic/partition/offset metadata for reliable deduplication.
- **Timestamps:** `EVENT_TIME` represents the historical event time; `SIMULATED_AT` represents the replay timestamp, not a verified Snowflake ingestion timestamp.
- **Revenue logic:** order status interpretation, recognized revenue, refunds, and profitability are downstream business decisions.

## Project Layout

```text
retail_dbt/
├── dbt_project.yml
├── models/
│   └── staging/
│       ├── sources.yml
│       ├── staging_models.yml
│       ├── stg_customers.sql
│       ├── stg_products.sql
│       ├── stg_orders.sql
│       ├── stg_order_items.sql
│       ├── stg_inventory.sql
│       └── stg_clickstream_events.sql
├── tests/
│   └── technical_quality/
│       └── (7 custom singular SQL tests)
├── macros/                 # Optional reusable SQL/Jinja logic
└── README.md
```

Standalone Snowflake monitoring SQL, if retained in the main repository, belongs outside `retail_dbt/models` (for example under `SQL_scripts/monitoring/`). Monitoring queries are read-only and on-demand; scheduling and dashboards are owned by the orchestration/analytics team.

## Setup

**Prerequisites:** Python 3.12, dbt Core, dbt Snowflake adapter, Snowflake access to the above database/warehouse, and a local dbt profile. The versions validated during development were `dbt-core==1.12.6` and `dbt-snowflake==1.12.4`.

1. From the repository root, activate your Python virtual environment. On Windows PowerShell, for example:

   ```powershell
   .\.venv\Scripts\Activate.ps1
   cd retail_dbt
   ```

   On Linux/macOS, use the environment activation command appropriate to your setup, then `cd retail_dbt`.

2. Configure a **local** `~/.dbt/profiles.yml` profile named `retail_dbt` (matching `dbt_project.yml`). Example structure:

   ```yaml
   retail_dbt:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: YOUR_ACCOUNT_IDENTIFIER
         user: YOUR_SNOWFLAKE_USER
         role: DBT_SILVER_ROLE
         database: RETAIL_CLICKSTREAM_DB
         warehouse: RETAIL_WH
         schema: STAGING
         threads: 4
         authenticator: username_password_mfa
         password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
   ```

   Replace the account/user/role/warehouse values as appropriate for your access. Never commit `profiles.yml` or passwords.

3. Set the `DBT_SNOWFLAKE_PASSWORD` environment variable locally for the current session. Avoid pasting credentials into source files or shared terminal logs.

4. Verify connectivity and project parsing:

   ```bash
   dbt debug
   dbt parse
   dbt ls --select path:models/staging --resource-type model
   ```

## Run and Validate

From inside `retail_dbt`:

```bash
# Build six staging views and execute their tests plus the custom technical checks
 dbt build --select path:models/staging path:tests/technical_quality

# Rerun validations without rebuilding views (useful after new RAW data arrives)
 dbt test --select path:models/staging path:tests/technical_quality

# Generate and inspect documentation and lineage
 dbt docs generate
 dbt docs serve --port 8080
```

The documentation site is available locally at `http://localhost:8080` while `dbt docs serve` is running. The generated `target/` artifacts should **not** be committed.

### Quality assurance

The project contains schema-based tests defined in `models/staging/staging_models.yml` and seven custom singular SQL tests under `tests/technical_quality/`. They check uniqueness, required identifiers, relationships, accepted values, arithmetic consistency, valid numbers, timestamps, and session/customer validity according to the implemented rules.

A singular test passes when its SQL returns **zero invalid rows**. An all-green dbt build confirms the tests run at that point in time; it is not an unconditional guarantee of future source quality.

## Last Verified Baseline — October 26, 2026

| Dataset | RAW rows | STAGING rows | Difference |
|---|---:|---:|---:|
| Customers | 3,022,290 | 3,022,290 | 0 |
| Products | 166,794 | 166,794 | 0 |
| Orders | 629,560 | 629,560 | 0 |
| Order Items | 690,372 | 690,372 | 0 |
| Inventory | 166,794 | 166,794 | 0 |
| Clickstream Events | 6,000 | 6,000 | 0 |

**Last successful dbt build:** 6 views + 35 data tests = **41 passed**, **0 warnings**, **0 errors**.

Additional verified observations:

- Missing product category codes: **93,644**; missing product brands: **43,613**.
- Missing clickstream category codes: **1,922**; missing clickstream brands: **792**.
- Zero-price products: **1,147**; zero-price clickstream events: **9**.
- Critical invalid-record checks: **0** in the tested categories.
- One clickstream payload appeared **4 times**, giving **3 additional identical occurrences**; **none were removed**.
- Last event timestamps matched between RAW and STAGING: historical `2019-10-01 02:30:06`, replay `2026-10-25 20:02:54`.

These are **observed baseline values, not hard-coded expectations**. Recheck them when the ingestion pipeline writes new rows.

## Streaming Behavior and Known Limitations

The Kafka producer/consumer is maintained separately by the ingestion team. The staging views automatically expose newly committed RAW rows, but:

1. Publishing to Kafka does not prove a consumer committed the events to Snowflake.
2. Ingestion append and retry/idempotency behavior still requires an end-to-end confirmation test with the ingestion owner.
3. The clickstream RAW table has no proven stable per-event ID or source offset; identical payloads should not be deduplicated blindly.
4. A replayed old `EVENT_TIME` must not be discarded simply because an event timestamp is older than the current maximum.
5. Automated tests and alerts require orchestration or scheduling; this repository supplies the tests, not the scheduler.

**Current decision:** no incremental dbt tables are required at this layer. Revisit materialization only if measured query performance/cost or ingestion guarantees justify it.

## Handoff to Data Warehouse / Gold

Downstream modeling should consume these **six staging views** using dbt `ref()` calls inside the shared project. Model names and source-level grain are the interface; consult `staging_models.yml`, `sources.yml`, and the generated dbt Docs for descriptions and lineage.

The Data Warehouse owner is responsible for:

- joins and aggregations;
- fact/dimension grain and surrogate keys;
- star schema and data marts;
- business KPI definitions and revenue semantics.

The Analytics/Orchestration owner is responsible for dashboards, insights, scheduling, and alerts. If a different physical schema is eventually required, coordinate naming and permissions before changing the existing dbt materializations.

## Security and Repository Hygiene

- Commit **source SQL, YAML tests, documentation, and dependency specifications**.
- Do **not** commit `profiles.yml`, passwords, `.env` secrets, `.venv/`, `target/`, `logs/`, or `dbt_packages/`.
- Review staged Git changes before pushing to the shared repository.
- This README describes the **dbt component only**, not the entire team pipeline.
