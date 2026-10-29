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
| `RAW_CLICKSTREAM_EVENTS` | `stg_clickstream_events` | One source event row | Preserve `EVENT_ID`; standardize event types; preserve historical/replay timestamps; create composite user/session key; quality flags |

The clickstream view includes the source-provided `EVENT_ID` alongside the derived `SESSION_KEY`; these have distinct purposes (event identity vs. session grouping). All six models preserve the **source row grain**. Cross-table joins, business aggregations, star-schema design, and KPI definitions are intentionally left to the downstream modeling team.

### Data handling policies

- **Missing category/brand:** retain source `NULL` values and expose display labels or missing-value flags where implemented. Missing descriptive fields do not trigger row deletion.
- **Zero values:** keep plausible values (such as out-of-stock inventory and zero-priced products) and flag them for interpretation; distinguish them from invalid negative values.
- **Clickstream event identity:** `EVENT_ID` (Snowflake `NUMBER(38,0)`) is supplied by the ingestion pipeline and passed through staging unchanged. The staging layer checks for missing and duplicate IDs, but does not delete or deduplicate events. For retry-safe deduplication, the producer/consumer must preserve the same ID for the same logical event across retries/replays. Different IDs can still describe a replay of the same logical source event.
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

Standalone Snowflake SQL is maintained outside the dbt model directory, under `SQL_scripts/silver/` in the shared repository. It is grouped by purpose (profiling, validation, monitoring, orchestration handoff). These are **read-only/on-demand SQL checks** and operational references, not dbt model replacements. Check the actual filenames in the repository; folder or worksheet organization does not itself schedule execution.

The canonical cleaning/transformation logic remains in `retail_dbt/models/staging/*.sql` and should be run through dbt (it contains Jinja such as `source()`), not pasted as standalone Snowflake SQL. The seven singular quality checks remain in `retail_dbt/tests/technical_quality/`.

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
# Deploy or update the clickstream view after a SQL model definition change
 dbt run --select stg_clickstream_events

# Validate the clickstream view (schema tests and its singular technical test)
 dbt test --select stg_clickstream_events invalid_clickstream_records

# Build all six staging views and run all project quality tests
 dbt build --select path:models/staging path:tests/technical_quality

# After new RAW rows arrive, rerun all tests WITHOUT rebuilding the views
 dbt test --select path:models/staging path:tests/technical_quality

# Generate and inspect documentation and lineage
 dbt docs generate
 dbt docs serve --port 8080
```

The documentation site is available locally at `http://localhost:8080` while `dbt docs serve` is running. The generated `target/` artifacts should **not** be committed.

### Quality assurance

The project defines **30 generic/schema tests** in `models/staging/staging_models.yml` (including `not_null` and `unique` on `EVENT_ID`) and **7 custom singular SQL tests** under `tests/technical_quality/`, for **37 configured tests** in total. They check uniqueness, required identifiers, relationships, accepted values, arithmetic consistency, valid numbers, timestamps, and session/customer validity according to the implemented rules.

A singular test passes when its SQL returns **zero invalid rows**. An all-green dbt build confirms the tests run at that point in time; it is not an unconditional guarantee of future source quality.

## Verification Status — October 27, 2026

### Current clickstream verification

The upstream RAW clickstream table was loaded with `EVENT_ID` as `NUMBER(38,0)`. The staging SQL model was updated to expose this ID while retaining `SESSION_KEY` and existing data-quality flags.

The following results were **observed in Snowflake and dbt on October 27, 2026**:

| Clickstream metric | Verified result |
|---|---:|
| RAW clickstream rows at inspection | 150,000 |
| STAGING clickstream rows | 150,000 |
| STAGING `EVENT_ID` values missing (`NULL`) | 0 |
| Distinct STAGING `EVENT_ID` values | 150,000 |
| Duplicate STAGING `EVENT_ID` rows | 0 |
| Clickstream-focused dbt tests executed | 12 |
| Clickstream-focused dbt tests passed | 12 |
| Clickstream test warnings / errors | 0 / 0 |

**Test scope matters:** the current dbt project has **37 configured quality tests** (30 generic + 7 singular), but the **latest observed post-change run covered only the 12 clickstream-related tests**. A full 37-test rerun after the new clickstream load has **not been established by these results**. A full `dbt build` would involve 6 view models plus 37 tests (43 operations) if all resources are selected and run successfully; **43 passed is not yet a claimed result**.

The matching counts and unique/non-null event IDs are valid for the observed snapshot. They do **not** by themselves prove exactly-once Kafka ingestion, idempotent replay, or that every source event has arrived.

### Prior OLTP baseline — October 26, 2026

The following non-clickstream sources were validated previously. Recheck the numbers if their RAW tables have since changed:

| Dataset | RAW rows | STAGING rows | Difference |
|---|---:|---:|---:|
| Customers | 3,022,290 | 3,022,290 | 0 |
| Products | 166,794 | 166,794 | 0 |
| Orders | 629,560 | 629,560 | 0 |
| Order Items | 690,372 | 690,372 | 0 |
| Inventory | 166,794 | 166,794 | 0 |

The earlier full build passed **6 views + 35 tests = 41 operations** before the two `EVENT_ID` tests were added. That historical result should not be presented as a current full-project test run.

Previously observed product issues included missing category codes (93,644), missing brands (43,613), and zero-price products (1,147). Earlier clickstream missing-value, payload-repeat, and timestamp counts belonged to a **superseded 6,000-event dataset** and must **not** be assumed to describe the new 150,000-event load.

All row counts and quality metrics are observed baselines, **not hard-coded expectations**. Recheck them after subsequent append/streaming activity.

## Streaming Behavior and Known Limitations

The Kafka producer/consumer is maintained separately by the ingestion team. The staging views automatically expose newly committed RAW rows, but:

1. Publishing to Kafka does not prove a consumer committed the events to Snowflake.
2. Ingestion append and retry/idempotency behavior still requires an end-to-end confirmation test with the ingestion owner.
3. `EVENT_ID` is now available in RAW and STAGING and was verified unique/non-null in the checked 150,000-row snapshot. Reliable retry/replay identity still depends on producer/consumer behavior: the same logical event must retain its ID on retry. Do not mistake the absence of duplicate IDs for proof of exactly-once delivery.
4. A replayed old `EVENT_TIME` must not be discarded simply because an event timestamp is older than the current maximum.
5. Automated tests and alerts require orchestration or scheduling; this repository supplies the tests, not the scheduler.

**Current decision:** no incremental dbt tables are required at this layer. Revisit materialization only if measured query performance/cost or ingestion guarantees justify it.

### Event ID and session key serve different purposes

- `EVENT_ID`: source-provided event-level identifier; can be used by downstream `FACT_CLICKSTREAM_EVENTS` subject to agreed identity/replay rules.
- `SESSION_KEY`: derived `SHA2` hash of the user ID and source session ID; multiple events in the same user session legitimately share it. It is **not** a substitute for `EVENT_ID`.

### Orchestration handoff

1. The ingestion owner must ensure the RAW tables are committed and the schema remains compatible. A Kafka producer success message alone is not sufficient.
2. The STAGING **views** reflect new RAW rows at query time; do **not** schedule `dbt run` merely because a new batch of rows arrives. Run it when model definitions change or during deliberate redeployment.
3. Schedule `dbt test --select path:models/staging path:tests/technical_quality` after ingestion commits or at the agreed cadence. The orchestration runtime needs dbt dependencies, a Snowflake profile/role, and securely provided credentials.
4. Run the read-only checks under `SQL_scripts/silver/` to inspect reconciliation, event-ID integrity, quality summaries, and replay timestamps. A query returning counts is monitoring evidence, **not** a built-in trigger or alert.
5. Refresh downstream Gold **tables** using the warehouse team's schedule/incremental logic, or query Gold **views** directly. Dashboard refresh cadence is configured separately; none of these are implemented by this dbt project.

**Runbook note:** After a source-table drop/recreation, verify permissions, source schema compatibility, and current rows, then update or rebuild only affected dbt models if their SQL definitions changed. Rerun relevant tests. There is no need to recreate unrelated staging views.

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
