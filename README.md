# etl-prometheus2clickhouse

ETL job that reads metrics from Prometheus, writes them to ClickHouse in batches and uses ClickHouse to store job state.

## Overview

The job:

- reads all metrics from Prometheus using `query_range` (always exports all metrics);
- writes rows into a single ClickHouse table:
  - `timestamp` (DateTime) – metric timestamp;
  - `metric_name` (String) – metric name;
  - `labels` (String) – JSON-encoded labels;
  - `value` (Float64) – metric value;
- stores job state in ClickHouse ETL table:
  - `timestamp_progress` – current processing progress timestamp;
  - `timestamp_start` – job start timestamp;
  - `timestamp_end` – job completion timestamp;
  - `batch_window_seconds` – size of processed window;
  - `batch_rows` – number of rows processed in batch.

All connection settings are provided via environment variables. Job state is
stored in ClickHouse ETL table.

**Important:** Before the first run, the `timestamp_progress` value must be
set in ClickHouse ETL table to specify the starting point for data processing.
The job will fail if this value is not found.

Note: `timestamp_start` and `timestamp_end` may be absent on the first
run (they will be created automatically). Only `timestamp_progress` is
required before the first execution.

## Requirements

- `uv` package manager

## Installation

```bash
make install
```

## Configuration

All connection settings are defined in environment variables. See `env.example`
for the full list. The most important variables:

- `PROMETHEUS_URL` – Prometheus/Mimir base URL;
  - Optional basic auth: set `PROMETHEUS_USER` and `PROMETHEUS_PASSWORD`;
  - Set `PROMETHEUS_INSECURE=1` to disable TLS verification;
- `CLICKHOUSE_URL` – ClickHouse HTTP URL (required);
  - `CLICKHOUSE_TABLE_METRICS` – table name for metrics (default: `default.metrics`);
  - `CLICKHOUSE_TABLE_ETL` – table name for ETL state (default: `default.etl`);
  - Optional: `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` for authentication;
  - Set `CLICKHOUSE_INSECURE=1` to disable TLS verification;
- `BATCH_WINDOW_SIZE_SECONDS` – processing window size in seconds;
- `BATCH_WINDOW_OVERLAP_SECONDS` – overlap in seconds to avoid missing data at
  boundaries;
- `LOG_LEVEL` – logging level (default: `INFO`).

## Running

Run locally:

```bash
make run
```

Run checks:

```bash
make all
```

### Debugging

For debugging purposes, you can run the ETL job periodically to monitor its
behavior:

```bash
while true; do make docker-run; sleep 10; done
```

This will run the job every 10 seconds. Adjust the sleep interval as needed.

## ClickHouse Table Requirements

The target ClickHouse tables must be configured to handle potential duplicate
inserts. This is required because the job is designed to be idempotent: if
state update fails after successful data write, the job will reprocess
the same window on the next run, which may result in duplicate rows.

You can use the default database or create a custom database. To create a custom
database:

```sql
CREATE DATABASE IF NOT EXISTS metrics;
```

Recommended table engine: `ReplacingMergeTree` or a table with deduplication
enabled. The table schemas should match the following structure:

Metrics table:

```sql
CREATE TABLE default.metrics (
    timestamp DateTime,
    metric_name String,
    labels String,
    value Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (timestamp, metric_name, labels);
```

ETL state table:

```sql
CREATE TABLE default.etl (
    timestamp_progress Nullable(Int64),
    timestamp_start Nullable(Int64),
    timestamp_end Nullable(Int64),
    batch_window_seconds Nullable(Int64),
    batch_rows Nullable(Int64)
) ENGINE = ReplacingMergeTree()
ORDER BY (timestamp_progress, timestamp_start, timestamp_end);
```

## Logging

Logging is implemented with `logging-objects-with-schema` and standard Python
logging formatters. Schema is defined in `logging_objects_with_schema.json`.

## Troubleshooting

### Job Stuck: TimestampStart Exists but TimestampEnd Missing

**Symptoms:**

- Job logs show: "Previous job is still running (TimestampStart exists but TimestampEnd is missing), skipping run"
- Job cannot start on subsequent runs

**Cause:**
This happens when the job successfully marks its start (`timestamp_start` is saved to ClickHouse) but fails before completing the batch (e.g., error loading `timestamp_progress`, network failure, or application crash). The job's safety mechanism prevents concurrent runs by checking that the previous run completed.

**Solution:**
Set `timestamp_end` value in ClickHouse ETL table to a value greater than `timestamp_start`.
This marks the previous job as completed and allows the new job to start:

```bash
TIMESTAMP_END=$(date +%s)
clickhouse-client --query "INSERT INTO default.etl (timestamp_end) VALUES ($TIMESTAMP_END)"
```

Or using HTTP interface:

```bash
TIMESTAMP_END=$(date +%s)
curl -X POST "http://clickhouse:8123/?query=INSERT+INTO+default.etl+(timestamp_end)+VALUES+($TIMESTAMP_END)"
```

**Note:** After setting `timestamp_end`, the job will be able to pass the start check on the next run. However, if this was the first run and the job never completed successfully, `timestamp_progress` may be missing. In that case, the job will fail with "TimestampProgress not found in ClickHouse" error. You will need to set `timestamp_progress` manually as described in the "TimestampProgress Not Found in ClickHouse" section below.

If there was a previous successful run, the job will continue from the last successful `timestamp_progress` value (which is stored in ClickHouse).

### TimestampProgress Not Found in ClickHouse

**Symptoms:**

- Job logs show: "TimestampProgress not found in ClickHouse"
- Job fails to start with error: "TimestampProgress not found in ClickHouse"

**Cause:**

This happens when:

- This is the first run and the job has never completed successfully (no `timestamp_progress` was ever set)
- The `timestamp_progress` value was deleted from ClickHouse ETL table
- The ETL table doesn't exist or is empty

**Solution:**

Set `timestamp_progress` value in ClickHouse ETL table to the Unix timestamp from which you want to start processing.

Convert any date/time to Unix timestamp:

Current time:

```bash
export TIMESTAMP_PROGRESS=$(date +%s)
```

Specific date (Linux):

```bash
export TIMESTAMP_PROGRESS=$(date -d "2024-01-01 00:00:00" +%s)
```

Specific date (macOS):

```bash
export TIMESTAMP_PROGRESS=$(date -j -f "%Y-%m-%d %H:%M:%S" "2024-01-01 00:00:00" +%s)
```

N days ago (Linux):

```bash
export TIMESTAMP_PROGRESS=$(date -d "30 days ago" +%s)
```

N days ago (macOS):

```bash
export TIMESTAMP_PROGRESS=$(date -v-30d +%s)
```

Set the value in ClickHouse:

```bash
clickhouse-client --query "INSERT INTO default.etl (timestamp_progress) VALUES ($TIMESTAMP_PROGRESS)"
```

Or using HTTP interface:

```bash
curl -X POST "http://clickhouse:8123/?query=INSERT+INTO+default.etl+(timestamp_progress)+VALUES+($TIMESTAMP_PROGRESS)"
```

**Note:** After setting `timestamp_progress`, the job will be able to start on the next run. The job will process data starting from
this timestamp.
