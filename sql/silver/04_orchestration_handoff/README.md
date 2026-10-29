# Orchestration teammate: Silver-layer runbook

**Source of truth for transformations:** the versioned `retail_dbt` directory in GitHub (not the Snowsight SQL monitoring folders). Existing `STG_*` objects are Snowflake views, already built by dbt. The 6 source-grain views do not need a `dbt run` every 5 minutes.

## Readiness / quality gates

- Query `01_silver_readiness.sql` to see whether clickstream count and EVENT_ID integrity currently look correct. The `READY_FOR_FURTHER_VALIDATION` status is NOT full quality approval.
- From a runner with dbt Core + dbt-snowflake, a valid user profile and secrets, run:

```bash
cd retail_dbt
dbt test --select stg_clickstream_events invalid_clickstream_records
```

- Run the full suite if required, but benchmark credit/runtime cost first (millions of OLTP rows can be scanned):

```bash
dbt test --select path:models/staging path:tests/technical_quality
```

- If a dbt model SQL definition is changed, perform `dbt run --select <model>` once before rerunning its tests. For new RAW rows with unchanged compatible schemas, views need no rebuild.
- Failures: stop/flag downstream loading according to team's SLA, inspect failing test and record IDs, do not delete rows blindly.

## Gold/dashboard dependencies

- Gold VIEWS: query latest staging data automatically; dashboard needs query refresh.
- Gold materialized TABLES/incremental dbt: trigger refresh AFTER source data and checks are ready; dashboard refresh should follow successful Gold update.
- If event identifiers are regenerated on every replay, even perfect uniqueness at one load does not prevent logical duplicates across batches. Work with Kafka producer owner on stable IDs/idempotency.

## Trigger vs schedule

This project has no automatic Snowflake stream/task or Airflow DAG configured in the Silver handoff. Deploying a Snowflake dbt Project or using an external Airflow dbt runner is a separate orchestration implementation, requiring its own roles and service credentials. Workspaces store code, not runnable scheduled pipelines by themselves.

## Permissions

Give the orchestrator role: appropriate USAGE on warehouse/database/schema, SELECT on sources and staging, and any additional permissions necessary for Gold. Shared Workspace READ/WRITE is independent from SQL query execution privileges.
