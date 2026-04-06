# Cofertility Conversion Funnel: dbt Project

A working dbt project that implements the data model proposed in the take-home assignment. Runs locally against DuckDB with the provided sample data.

## Quick Start

```bash
pip install dbt-duckdb
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Architecture

```
seeds/              Raw CSV data (from the provided .sql file)
models/
  staging/          Clean, rename, type-cast, deduplicate
    stg_events      Extract JSON properties, filter retries
    stg_sessions    Normalize UTMs, derive channel_group
    stg_conversions Deduplicate by user_id, normalize plan_type
  intermediate/     Identity resolution, funnel pivoting
    int_session_user_mapping    Bridge: session_id <> user_id
    int_user_first_touch        First-touch attribution spine
    int_funnel_events_aggregated  Funnel steps per session
  marts/            Business-facing, ready for Metabase
    fct_session_funnel          One row per session, full funnel + attribution
    fct_channel_performance     One row per channel per month, with CAC
```

## Key Design Decisions

1. **Session is the grain** for funnel analysis (pre-signup users are anonymous).
2. **Orphan sessions preserved** as 'unattributable' rather than silently dropped.
3. **Both attribution models** (first-touch channel + session-level channel) available in the mart.
4. **Deduplication in staging**, not in marts. The mart is a clean join.

## Tests

18 data quality tests covering uniqueness, not-null constraints, and accepted values across all layers.
