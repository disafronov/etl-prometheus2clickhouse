# etl-prometheus2clickhouse

ETL job that reads metrics from Prometheus/Mimir (Prometheus-compatible API),
writes them to ClickHouse in batches and uses PushGateway to store job
state metrics.

## Overview

The job:

- reads all metrics from Prometheus using `query_range` (always exports all metrics);
- writes rows into a single ClickHouse table:
  - `timestamp` (DateTime) – metric timestamp;
  - `metric_name` (String) – metric name;
  - `labels` (String) – JSON-encoded labels;
  - `value` (Float64) – metric value;
- uses Prometheus metrics:
  - `etl_timestamp_start`,
  - `etl_timestamp_end`,
  - `etl_timestamp_progress`;
- pushes the same timestamps and batch metadata to PushGateway:
  - `etl_timestamp_start`,
  - `etl_timestamp_end`,
  - `etl_timestamp_progress`,
  - `etl_batch_window_seconds`,
  - `etl_batch_rows`.

All connection settings are provided via environment variables. Job state is
read only from Prometheus, PushGateway is write-only.

**Important:** Before the first run, the `etl_timestamp_progress` metric must be
set in Prometheus to specify the starting point for data processing. The job
will fail if this metric is not found.

Note: `etl_timestamp_start` and `etl_timestamp_end` may be absent on the first
run (they will be created automatically). Only `etl_timestamp_progress` is
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
- `CLICKHOUSE_URL`, `CLICKHOUSE_TABLE` – ClickHouse HTTP URL and table name;
  - Optional: `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` for authentication;
  - Set `CLICKHOUSE_INSECURE=1` to disable TLS verification;
- `PUSHGATEWAY_URL`, `PUSHGATEWAY_JOB`, `PUSHGATEWAY_INSTANCE` – PushGateway
  endpoint and labels;
  - Optional: `PUSHGATEWAY_TOKEN` (Bearer) or `PUSHGATEWAY_USER`/`PUSHGATEWAY_PASS` (Basic auth);
  - Set `PUSHGATEWAY_INSECURE=1` to disable TLS verification;
- `BATCH_WINDOW_SECONDS` – processing window size in seconds;
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

## ClickHouse Table Requirements

The target ClickHouse table must be configured to handle potential duplicate
inserts. This is required because the job is designed to be idempotent: if
PushGateway update fails after successful data write, the job will reprocess
the same window on the next run, which may result in duplicate rows.

First, create the database (if it doesn't exist):

```sql
CREATE DATABASE IF NOT EXISTS metrics;
```

Recommended table engine: `ReplacingMergeTree` or a table with deduplication
enabled. The table schema should match the following structure:

```sql
CREATE TABLE metrics.prometheus_raw (
    timestamp DateTime,
    metric_name String,
    labels String,
    value Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (timestamp, metric_name, labels);
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
This happens when the job successfully marks its start (`timestamp_start` is pushed to PushGateway) but fails before completing the batch (e.g., error loading `timestamp_progress`, network failure, or application crash). The job's safety mechanism prevents concurrent runs by checking that the previous run completed.

**Solution:**
Set `etl_timestamp_end` metric in PushGateway to a value greater than `etl_timestamp_start`.
This marks the previous job as completed and allows the new job to start:

```bash
TIMESTAMP_END=$(date +%s)
curl -X POST http://pushgateway:9091/metrics/job/etl_prometheus2clickhouse/instance/etl_prometheus2clickhouse \
  -d "# TYPE etl_timestamp_end gauge
etl_timestamp_end $TIMESTAMP_END
"
```

If PushGateway requires authentication, add credentials:

Basic Auth:

```bash
TIMESTAMP_END=$(date +%s)
curl -X POST -u user:password http://pushgateway:9091/metrics/job/etl_prometheus2clickhouse/instance/etl_prometheus2clickhouse \
  -d "# TYPE etl_timestamp_end gauge
etl_timestamp_end $TIMESTAMP_END
"
```

Bearer Token:

```bash
TIMESTAMP_END=$(date +%s)
curl -X POST -H "Authorization: Bearer TOKEN" http://pushgateway:9091/metrics/job/etl_prometheus2clickhouse/instance/etl_prometheus2clickhouse \
  -d "# TYPE etl_timestamp_end gauge
etl_timestamp_end $TIMESTAMP_END
"
```

**Note:** After setting `timestamp_end`, wait a few seconds for Prometheus to scrape the updated
metric from PushGateway, then the job will be able to start. The job will continue from the last
successful `timestamp_progress` value (which is stored in Prometheus).
