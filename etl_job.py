#!/usr/bin/env python3
"""
Core ETL job implementation.

Implements the ETL algorithm:
- Check start condition via TimestampStart/TimestampEnd from Prometheus.
- Mark start by pushing new TimestampStart to PushGateway.
- Read TimestampProgress from Prometheus (required, job fails if not found).
- Calculate processing window based on progress and batch window size.
- Fetch data from Prometheus and write to ClickHouse in a single batch.
- Push updated progress and end timestamps, plus window size and rows count.
"""

from __future__ import annotations

import json
import time
from typing import Any

from clickhouse_client import ClickHouseClient
from config import Config
from logging_config import getLogger
from prometheus_client import PrometheusClient
from pushgateway_client import PushGatewayClient

logger = getLogger(__name__)


class EtlJob:
    """ETL job coordinator.

    Coordinates the ETL process: reads job state from Prometheus, processes
    data in batches, writes to ClickHouse, and updates state via PushGateway.
    Uses dependency injection for all external services to enable testing.
    """

    def __init__(
        self,
        config: Config,
        prometheus_client: PrometheusClient,
        clickhouse_client: ClickHouseClient,
        pushgateway_client: PushGatewayClient,
    ) -> None:
        """Initialize ETL job with all required dependencies.

        Args:
            config: Application configuration (timeouts, batch size, etc.)
            prometheus_client: Client for reading job state and metrics
            clickhouse_client: Client for writing processed data
            pushgateway_client: Client for updating job state metrics
        """
        self._config = config
        self._prom = prometheus_client
        self._ch = clickhouse_client
        self._pg = pushgateway_client

    def run_once(self) -> None:
        """Run single ETL iteration according to the algorithm.

        Executes one complete ETL cycle:
        1. Checks if job can start (prevents concurrent runs)
        2. Marks start in PushGateway (atomic operation to claim execution)
        3. Loads progress from Prometheus (required, fails if missing)
        4. Fetches and writes data for current window
        5. Updates progress and end timestamp only after successful write

        Raises:
            ValueError: If TimestampProgress is not found in Prometheus.
            Exception: If any step fails (fetch, write, or push metrics).
        """
        if not self._check_can_start():
            return

        timestamp_start = time.time()
        if not self._mark_start(timestamp_start):
            return

        progress = self._load_progress()
        window_start, window_end = self._calc_window(progress)

        rows = self._fetch_data(window_start, window_end)
        self._write_rows(rows)

        new_progress = progress + self._config.etl.batch_window_seconds
        # Ensure timestamp_end is always greater than timestamp_start.
        # TimestampEnd must never equal TimestampStart.
        timestamp_end = max(time.time(), timestamp_start + 0.001)

        self._push_metrics_after_success(
            timestamp_end=timestamp_end,
            timestamp_progress=new_progress,
            window_seconds=self._config.etl.batch_window_seconds,
            rows_count=len(rows),
        )

    def _read_gauge(self, metric_name: str) -> float | None:
        """Read single gauge value from Prometheus instant query result.

        Uses instant query (not range query) to get current value of a gauge metric.
        Returns None if metric doesn't exist or query fails, allowing caller to
        distinguish between "metric missing" and "metric has value 0".

        Args:
            metric_name: Name of the gauge metric to read

        Returns:
            Metric value as float, or None if metric doesn't exist or query failed
        """
        data = self._prom.query(metric_name)
        status = data.get("status")
        if status != "success":
            return None

        result = data.get("data", {}).get("result", [])
        if not result:
            return None

        try:
            value = result[0]["value"][1]
            return float(value)
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    def _check_can_start(self) -> bool:
        """Check if job is allowed to start based on timestamps.

        Prevents concurrent job execution by checking previous run state:
        - Both timestamps missing: first run, allow start
        - End exists but start missing: inconsistent state, but previous job finished
          (has end timestamp), allow start with warning
        - Start exists but end missing: previous job still running, block start
        - End < Start: previous job still running (end not updated yet), block start
        - End > Start: previous job completed, allow start

        Returns:
            True if job can start, False otherwise
        """
        try:
            ts_start = self._read_gauge("etl_timestamp_start")
            ts_end = self._read_gauge("etl_timestamp_end")

            # Both missing - first run, allow start
            if ts_start is None and ts_end is None:
                return True

            # End exists but start doesn't - inconsistent state,
            # but previous job finished, allow start with warning
            if ts_start is None and ts_end is not None:
                logger.warning(
                    (
                        "Inconsistent state: TimestampEnd exists but "
                        "TimestampStart is missing"
                    ),
                    extra={
                        "etl_job.check_start_warning.message": (
                            "TimestampEnd exists but TimestampStart is missing; "
                            "previous job appears to have finished, allowing start"
                        ),
                    },
                )
                return True

            # Start exists but end doesn't - previous job is still running
            if ts_start is not None and ts_end is None:
                logger.warning(
                    (
                        "Previous job is still running "
                        "(TimestampStart exists but TimestampEnd is missing), "
                        "skipping run"
                    ),
                    extra={
                        "etl_job.check_start_failed.message": (
                            "TimestampStart exists but TimestampEnd is missing; "
                            "previous job is still running, job will not start"
                        ),
                    },
                )
                return False

            # Both exist - check their relationship
            # Explicit None checks ensure both values are not None before comparison
            if ts_end is not None and ts_start is not None and ts_end < ts_start:
                logger.warning(
                    "Previous job is still running or ended incorrectly, skipping run",
                    extra={
                        "etl_job.check_start_failed.message": (
                            "TimestampEnd is less than TimestampStart; "
                            "job will not start"
                        ),
                    },
                )
                return False

            return True
        except Exception as exc:
            logger.error(
                "Failed to check start condition",
                extra={
                    "etl_job.check_start_failed.message": str(exc),
                },
            )
            return False

    def _mark_start(self, timestamp_start: float) -> bool:
        """Push TimestampStart to PushGateway.

        Marks the start of job execution atomically. This must succeed before
        processing begins to prevent concurrent runs. If this fails, job stops
        immediately without processing any data.

        Args:
            timestamp_start: Unix timestamp when job started

        Returns:
            True if start was marked successfully, False otherwise
        """
        try:
            self._pg.push_start(timestamp_start)
            return True
        except Exception as exc:
            logger.error(
                "Failed to mark job start",
                extra={
                    "etl_job.mark_start_failed.message": str(exc),
                },
            )
            return False

    def _load_progress(self) -> float:
        """Load TimestampProgress from Prometheus.

        Reads the current processing progress timestamp. This metric must be
        set before the first run to specify the starting point. Job does not
        attempt to auto-detect the oldest metric to avoid overloading Prometheus
        with expensive queries.

        Returns:
            Current progress timestamp as Unix timestamp

        Raises:
            ValueError: If TimestampProgress is not found in Prometheus.
            Exception: If reading from Prometheus fails.
        """
        try:
            progress = self._read_gauge("etl_timestamp_progress")
            if progress is not None:
                return progress
        except Exception as exc:
            logger.error(
                "Failed to read TimestampProgress from Prometheus",
                extra={
                    "etl_job.load_progress_failed.message": str(exc),
                },
            )
            raise

        # TimestampProgress not found - this is a fatal error
        logger.error(
            "TimestampProgress metric not found in Prometheus",
            extra={
                "etl_job.load_progress_failed.message": (
                    "etl_timestamp_progress metric is required but not found. "
                    "Job cannot proceed without initial progress timestamp."
                ),
            },
        )
        raise ValueError(
            "TimestampProgress (etl_timestamp_progress) not found in Prometheus. "
            "Job requires this metric to determine starting point."
        )

    def _calc_window(self, progress: float) -> tuple[float, float]:
        """Calculate processing window based on progress and config.

        Determines the time range to fetch from Prometheus for this batch.
        Window starts at current progress and extends by batch_window_seconds.

        Args:
            progress: Current progress timestamp (start of window)

        Returns:
            Tuple of (window_start, window_end) as Unix timestamps
        """
        window_size = float(self._config.etl.batch_window_seconds)
        return progress, progress + window_size

    def _fetch_data(
        self, window_start: float, window_end: float
    ) -> list[dict[str, Any]]:
        """Fetch data from Prometheus for given window.

        Queries all metrics using {__name__=~".+"} selector to export everything
        available in Prometheus. This ensures complete metric export regardless
        of metric names or labels.

        Args:
            window_start: Start of time range (Unix timestamp)
            window_end: End of time range (Unix timestamp)

        Returns:
            List of rows, each containing timestamp (int Unix timestamp),
            metric_name, labels (JSON), and value

        Raises:
            Exception: If Prometheus query fails
        """
        try:
            step = f"{self._config.prometheus.query_step_seconds}s"
            # Always query all metrics: {__name__=~".+"}
            data = self._prom.query_range(
                '{__name__=~".+"}',
                start=window_start,
                end=window_end,
                step=step,
            )
        except Exception as exc:
            logger.error(
                "Failed to fetch data from Prometheus",
                extra={
                    "etl_job.fetch_failed.message": str(exc),
                },
            )
            raise

        result = data.get("data", {}).get("result", [])
        rows: list[dict[str, Any]] = []

        for series in result:
            metric = series.get("metric", {})
            metric_name = metric.get("__name__", "")
            labels = metric.copy()
            labels.pop("__name__", None)

            for value_pair in series.get("values", []):
                try:
                    # ClickHouse DateTime requires integer Unix timestamp
                    # Prometheus API returns timestamp as integer
                    # (whole seconds) in JSON
                    ts = int(value_pair[0])
                    value = float(value_pair[1])
                except (TypeError, ValueError, IndexError) as exc:
                    logger.warning(
                        "Skipping invalid value pair in Prometheus response",
                        extra={
                            "etl_job.invalid_value_pair.metric_name": metric_name,
                            "etl_job.invalid_value_pair.value_pair": str(value_pair),
                            "etl_job.invalid_value_pair.error": str(exc),
                            "etl_job.invalid_value_pair.error_type": type(exc).__name__,
                        },
                    )
                    continue

                row = {
                    "timestamp": ts,
                    "metric_name": metric_name,
                    "labels": labels,
                    "value": value,
                }
                rows.append(row)

        return rows

    def _write_rows(self, rows: list[dict[str, Any]]) -> None:
        """Write rows to ClickHouse in a single batch.

        Performs atomic batch insert. If this fails, no progress is updated,
        allowing job to retry the same window on next run. Empty rows list
        is handled gracefully (no-op).

        Args:
            rows: List of rows to insert, each with timestamp (int Unix timestamp),
                metric_name, labels (JSON string), and value

        Raises:
            Exception: If ClickHouse insert fails
        """
        try:
            self._ch.insert_rows(
                [
                    {
                        "timestamp": row["timestamp"],
                        "metric_name": row["metric_name"],
                        "labels": self._serialize_labels(row["labels"]),
                        "value": row["value"],
                    }
                    for row in rows
                ]
            )
        except Exception as exc:
            logger.error(
                "Failed to write rows to ClickHouse",
                extra={
                    "etl_job.write_failed.message": str(exc),
                },
            )
            raise

    @staticmethod
    def _serialize_labels(labels: dict[str, Any]) -> str:
        """Serialize labels dictionary to JSON string.

        Converts metric labels to compact JSON format (no spaces) for storage
        in ClickHouse String column. This preserves all label information while
        keeping storage efficient.

        Args:
            labels: Dictionary of label key-value pairs

        Returns:
            JSON string representation of labels
        """
        return json.dumps(labels, separators=(",", ":"))

    def _push_metrics_after_success(
        self,
        timestamp_end: float,
        timestamp_progress: float,
        window_seconds: int,
        rows_count: int,
    ) -> None:
        """Push progress and batch metrics to PushGateway.

        Updates job state only after successful data write. This ensures
        progress advances only when data is actually persisted. If this fails,
        progress is not updated, but data is already in ClickHouse, so next
        run will process the same window again (idempotent behavior).

        Args:
            timestamp_end: Job completion timestamp
            timestamp_progress: New progress timestamp (old + window_size)
            window_seconds: Size of processed window (for monitoring)
            rows_count: Number of rows processed (for monitoring)

        Raises:
            Exception: If PushGateway push fails
        """
        try:
            self._pg.push_success(
                timestamp_end=timestamp_end,
                timestamp_progress=timestamp_progress,
                window_seconds=window_seconds,
                rows_count=rows_count,
            )
        except Exception as exc:
            logger.error(
                "Failed to push metrics after successful batch",
                extra={
                    "etl_job.push_metrics_failed.message": str(exc),
                },
            )
            raise
