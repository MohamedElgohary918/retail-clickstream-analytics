# Silver SQL Backups & Orchestration Handoff

This folder contains READ-ONLY Snowflake SQL checks based on the existing project profiling, validation, and monitoring SQL packs, updated to use the current EVENT_ID-enabled clickstream schema.

**Important scope note:** These files are rebuilt from the earlier local SQL packs that were prepared in this project. They are **not a byte-for-byte export of the currently open Snowsight worksheets**. If a worksheet was edited later, download/copy the LIVE worksheet in Snowsight and replace the corresponding SQL file after reviewing the diff. This pack contains no credentials and no writes.

## Folder guide

- `01_data_profiling/`: Six profiling steps from the earlier SQL pack, split into files, plus a NEW EVENT_ID baseline query. The historical Q14 payload-comparison query is diagnostic only.
- `02_data_validation/`: RAW-to-STAGING reconciliation, Standardization, standalone Phase 3 technical validation, EVENT_ID validation.
- `03_technical_monitoring/`: Six primary monitors; Monitor 05 now checks EVENT_ID. Two optional diagnostics (payload similarity and Query History).
- `04_orchestration_handoff/`: Small readiness query and a runbook for the orchestration teammate.

## Important current findings (from screenshots, 2026-10-27)

- The refreshed `RAW_CLICKSTREAM_EVENTS` has a numeric `EVENT_ID` column.
- STAGING contains the new EVENT_ID field.
- Last displayed event counts: 150,000 rows; 0 missing event IDs; 0 duplicated event ID rows in the STAGING monitor.
- Last displayed selected clickstream test run: 12 PASS, 0 WARN, 0 ERROR. The entire 37-test project was **not** rerun in that screenshot.
- These values may change as Kafka continues to append. NEVER hardcode counts into alerts without agreeing on a refresh expectation.

## GitHub: add only the new Silver SQL folder

1. In Windows Explorer: unzip the archive into the local repo root. The archive paths start with `SQL_scripts/silver/`.
2. In the repo root, run `git status` and review `git diff` for modified tracked files.
3. Stage explicitly: `git add SQL_scripts/silver/` and your approved `retail_dbt` changes.
4. Commit and push your own branch; open/update your existing pull request. The ingestion teammate's `scripts/`, `streaming/`, and other `SQL_scripts/` files must remain unchanged.
5. Do NOT stage `.venv/`, `profiles.yml`, `.env`, `target/`, credentials, or locally downloaded datasets.

## Snowflake Workspaces

Use the Snowsight UI to create **one shared workspace** (for example `RETAIL_SILVER_SHARED`) with folders matching `01_data_profiling/`, `02_data_validation/`, `03_technical_monitoring/`, and `04_orchestration_handoff/`.

If you currently see **My Workspace**, that is private, not shared. A Snowflake role with `CREATE WORKSPACE` on a schema and `USAGE` on the database/schema is needed to create a shared workspace. Grant teammate roles `READ` or `WRITE` on the workspace as appropriate. A shared workspace SQL file only saves code, it does not automatically schedule the query.

Upload/copy the files into the folders, or use 'Move file to shared workspace' from an existing personal SQL file. Prefer **Copy** if you wish to preserve the private original. When editing files in a shared workspace, PUBLISH changes for teammates to see them. They need separate privileges on RAW/STAGING and warehouse to execute queries.

## What the orchestrator actually runs

`dbt` models are ALREADY in `retail_dbt/models/staging` on GitHub. They contain Jinja `{{ source(...) }}` macros and must be compiled/executed using dbt. Copying those files into a generic Snowflake SQL worksheet is not a substitute for a dbt run.

Staging models are Snowflake **VIEWS**. After Kafka consumer writes to RAW, the view queries see the new data; no repeated `dbt run` is needed unless the SQL model definition changes or the underlying schema changes.

For an external Airflow/Prefect runner with the repo checked out and dbt installed/configured:

```bash
cd retail_dbt
dbt debug
dbt test --select stg_clickstream_events invalid_clickstream_records
# Optional full quality suite after batch, or scheduled at a justified cadence:
dbt test --select path:models/staging path:tests/technical_quality
```

Each runner needs its own safe Snowflake credentials, role/warehouse access, and installed dbt dependencies. Do NOT copy a developer's `profiles.yml` or password to GitHub. For Snowflake-native orchestration, a **deployed Snowflake dbt Project object** must first exist; simply saving a local dbt project as Workspace SQL files is not deployment.

Suggested logical order (owners: ingestion/orchestration/warehouse/dashboard):

```
Kafka producer + consumer -> verified RAW arrival -> STAGING views
   -> tests/readiness gate (when scheduled) -> GOLD refresh (only if materialized)
   -> dashboard query/refresh (for example 5 minutes)
```

If GOLD is itself a view, it does not need a separate dbt refresh to see new RAW data. If GOLD is a stored table or incremental dbt model, it needs a scheduled update. Coordinate the dashboard refresh timing so it does not run before the GOLD update completes.

Caution: These monitoring SQL files are **on-demand checks**, NOT a live CDC-trigger integration. A schedule, sensor, or Snowflake Streams/Tasks integration must be implemented/tested independently by the orchestration teammate.

## Last pre-demo checklist

- `SHOW VIEWS IN SCHEMA RETAIL_CLICKSTREAM_DB.STAGING;` shows 6 views.
- Monitor 05: `MISSING_EVENT_IDS=0`, `DUPLICATE_EVENT_ID_ROWS=0` for the current load.
- Monitor 01: RAW/STAGING counts match for all six sources at the time checked.
- Run the 12 selected Clickstream tests after ingestion; a complete quality suite is a separate run.
- Confirm producer's EVENT_ID is **stable when replaying the same source event**; `unique` tests alone do not establish this.
- Check data freshness and dashboard behavior from the actual end-to-end demo, not from the dbt test summary alone.
