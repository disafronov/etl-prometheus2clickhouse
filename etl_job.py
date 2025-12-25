#!/usr/bin/env python3
"""
Core ETL job implementation.

Implements the ETL algorithm:
- Check start condition via TimestampStart/TimestampEnd from ClickHouse.
- Mark start by saving new TimestampStart to ClickHouse.
- Read TimestampProgress from ClickHouse (required, job fails if not found).
- Calculate processing window based on progress and batch window size.
- Fetch data from Prometheus and write to ClickHouse in a single batch.
- Save updated progress and end timestamps, plus window size and rows count
  to ClickHouse.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

from clickhouse_client import ClickHouseClient
from config import Config
from logging_config import getLogger
from prometheus_client import PrometheusClient

logger = getLogger(__name__)


class EtlJob:
    """ETL job coordinator.

    Coordinates the ETL process: reads job state from ClickHouse, processes
    data in batches, writes to ClickHouse, and updates state in ClickHouse.
    Uses dependency injection for all external services to enable testing.
    """

    def __init__(
        self,
        config: Config,
        prometheus_client: PrometheusClient,
        clickhouse_client: ClickHouseClient,
    ) -> None:
        """Initialize ETL job with all required dependencies.

        Args:
            config: Application configuration (timeouts, batch size, etc.)
            prometheus_client: Client for reading metrics data from Prometheus
            clickhouse_client: Client for writing processed data and job state
        """
        self._config = config
        self._prom = prometheus_client
        self._ch = clickhouse_client

    def run_once(self) -> None:
        """Run single ETL iteration according to the algorithm.

        Executes one complete ETL cycle:
        1. Checks if job can start (prevents concurrent runs)
        2. Marks start in ClickHouse (atomic operation to claim execution)
        3. Loads progress from ClickHouse (required, fails if missing)
        4. Fetches and writes data for current window
        5. Updates progress and end timestamp only after successful write

        Raises:
            ValueError: If TimestampProgress is not found in ClickHouse.
            Exception: If any step fails (fetch, write, or save state).
        """
        if not self._check_can_start():
            return

        logger.info("Job can start, beginning ETL cycle")

        timestamp_start = int(time.time())
        if not self._mark_start(timestamp_start):
            return

        logger.info(f"Job start marked at {int(timestamp_start)}")
        logger.info(
            f"Batch window size: {self._config.etl.batch_window_size_seconds}s, "
            f"batch window overlap: {self._config.etl.batch_window_overlap_seconds}s"
        )

        progress = self._load_progress()
        window_start, window_end = self._calc_window(progress)

        logger.info(f"Processing window: {window_start} - {window_end}")

        file_path, rows_count = self._fetch_data(window_start, window_end)
        try:
            self._write_rows(file_path)
        finally:
            # Always clean up temporary file, even if write fails
            try:
                os.unlink(file_path)
            except Exception:  # nosec B110
                # Ignore cleanup errors (file may not exist or already deleted)
                pass

        # Calculate new progress, but never exceed current time to avoid going
        # into the future where Prometheus has no data yet
        current_time = int(time.time())
        expected_progress = progress + self._config.etl.batch_window_size_seconds
        new_progress = min(expected_progress, current_time)
        actual_window = new_progress - progress

        # Log if window was reduced due to current time limit
        if new_progress < expected_progress:
            window_reduced = int(expected_progress - new_progress)
            logger.warning(
                f"Progress limited by current time: expected={int(expected_progress)}, "
                f"actual={int(new_progress)}, window_reduced_by={window_reduced}s"
            )

        # Ensure timestamp_end is always greater than timestamp_start.
        # TimestampEnd must never equal TimestampStart.
        timestamp_end = max(current_time, timestamp_start + 1)

        self._push_metrics_after_success(
            timestamp_start=timestamp_start,
            timestamp_end=timestamp_end,
            timestamp_progress=new_progress,
            window_seconds=int(actual_window),
            rows_count=rows_count,
        )

    def _read_state_field(self, field_name: str) -> int | None:
        """Read single state field from ClickHouse.

        Replaces _read_gauge() for reading from ClickHouse instead of Prometheus.
        Returns None if field doesn't exist, allowing caller to distinguish
        between "field missing" and "field has value 0".

        Args:
            field_name: Name of state field to read (timestamp_progress,
                timestamp_start, timestamp_end, etc.)

        Returns:
            Field value as int (Unix timestamp in seconds), or None if not set
        """
        try:
            state = self._ch.get_state()
            return state.get(field_name)
        except Exception as exc:
            logger.error(
                f"Failed to read state field {field_name} from ClickHouse",
                extra={
                    "etl_job.read_state_failed.field": field_name,
                    "etl_job.read_state_failed.message": str(exc),
                },
            )
            raise

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
            ts_start = self._read_state_field("timestamp_start")
            ts_end = self._read_state_field("timestamp_end")

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

    def _mark_start(self, timestamp_start: int) -> bool:
        """Save TimestampStart to ClickHouse.

        Marks the start of job execution atomically. This must succeed before
        processing begins to prevent concurrent runs. If this fails, job stops
        immediately without processing any data.

        Args:
            timestamp_start: Unix timestamp when job started (int, seconds since epoch)

        Returns:
            True if start was marked successfully, False otherwise
        """
        try:
            self._ch.save_state(timestamp_start=timestamp_start)
            return True
        except Exception as exc:
            logger.error(
                "Failed to mark job start",
                extra={
                    "etl_job.mark_start_failed.message": str(exc),
                },
            )
            return False

    def _load_progress(self) -> int:
        """Load TimestampProgress from ClickHouse.

        Reads the current processing progress timestamp. This value must be
        set before the first run to specify the starting point. Job does not
        attempt to auto-detect the oldest metric to avoid overloading Prometheus
        with expensive queries.

        Returns:
            Current progress timestamp as Unix timestamp (int, seconds since epoch)

        Raises:
            ValueError: If TimestampProgress is not found in ClickHouse.
            Exception: If reading from ClickHouse fails.
        """
        try:
            progress = self._read_state_field("timestamp_progress")
            if progress is not None:
                logger.info(f"Loaded progress timestamp: {progress}")
                return progress
        except Exception as exc:
            logger.error(
                "Failed to read TimestampProgress from ClickHouse",
                extra={
                    "etl_job.load_progress_failed.message": str(exc),
                },
            )
            raise

        # TimestampProgress not found - this is a fatal error
        logger.error(
            "TimestampProgress not found in ClickHouse",
            extra={
                "etl_job.load_progress_failed.message": (
                    "timestamp_progress is required but not found. "
                    "Job cannot proceed without initial progress timestamp."
                ),
            },
        )
        raise ValueError(
            "TimestampProgress not found in ClickHouse. "
            "Job requires this value to determine starting point."
        )

    def _calc_window(self, progress: int) -> tuple[int, int]:
        """Calculate processing window based on progress and config.

        Determines the time range to fetch from Prometheus for this batch.
        Window starts at (progress - overlap) to create overlap and extends by
        batch_window_size_seconds.

        Args:
            progress: Current progress timestamp (start of window without overlap, int)

        Returns:
            Tuple of (window_start, window_end) as Unix timestamps (int)
        """
        overlap = self._config.etl.batch_window_overlap_seconds
        window_size = self._config.etl.batch_window_size_seconds
        window_start = progress - overlap
        window_end = progress + window_size
        return window_start, window_end

    def _fetch_data(self, window_start: int, window_end: int) -> tuple[str, int]:
        """Fetch data from Prometheus for given window and write to JSONL file.

        Queries all metrics using {__name__=~".+"} selector to export everything
        available in Prometheus. Writes data to temporary JSONL file in streaming
        fashion to minimize memory usage. Each line contains a JSON object with
        timestamp, metric_name, labels (as JSON string), and value.

        Args:
            window_start: Start of time range (Unix timestamp, int)
            window_end: End of time range (Unix timestamp, int)

        Returns:
            Tuple of (file_path, rows_count) where file_path is path to JSONL file
            and rows_count is number of rows written

        Raises:
            Exception: If Prometheus query fails or file write fails
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

        if not result:
            logger.warning(
                "No metrics found in Prometheus for the specified time window",
                extra={
                    "etl_job.fetch_data_empty_result.window_start": window_start,
                    "etl_job.fetch_data_empty_result.window_end": window_end,
                    "etl_job.fetch_data_empty_result.step": step,
                },
            )
            # Create empty file to maintain consistent interface
            temp_dir = Path(self._config.etl.temp_dir)
            temp_dir.mkdir(parents=True, exist_ok=True)
            fd, file_path = tempfile.mkstemp(
                suffix=".jsonl", dir=temp_dir, prefix="etl_batch_"
            )
            os.close(fd)
            return file_path, 0

        # Create temporary file for streaming write
        temp_dir = Path(self._config.etl.temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        fd, file_path = tempfile.mkstemp(
            suffix=".jsonl", dir=temp_dir, prefix="etl_batch_"
        )

        rows_count = 0
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
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
                                    "etl_job.invalid_value_pair.metric_name": (
                                        metric_name
                                    ),
                                    "etl_job.invalid_value_pair.value_pair": str(
                                        value_pair
                                    ),
                                    "etl_job.invalid_value_pair.error": str(exc),
                                    "etl_job.invalid_value_pair.error_type": type(
                                        exc
                                    ).__name__,
                                },
                            )
                            continue

                        # Serialize labels to JSON string for ClickHouse
                        labels_json = self._serialize_labels(labels)

                        # Write row as JSON line (JSONEachRow format for ClickHouse)
                        row = {
                            "timestamp": ts,
                            "metric_name": metric_name,
                            "labels": labels_json,
                            "value": value,
                        }
                        f.write(json.dumps(row, separators=(",", ":")) + "\n")
                        rows_count += 1
        except Exception as exc:
            # Clean up file on error
            try:
                os.unlink(file_path)
            except Exception:  # nosec B110
                # Ignore cleanup errors
                pass
            logger.error(
                "Failed to write data to temporary file",
                extra={
                    "etl_job.fetch_write_failed.message": str(exc),
                    "etl_job.fetch_write_failed.file_path": file_path,
                },
            )
            raise

        logger.info(
            f"Parsed {rows_count} data points from {len(result)} metric series",
            extra={
                "etl_job.fetch_data_success.series_count": len(result),
                "etl_job.fetch_data_success.rows_count": rows_count,
                "etl_job.fetch_data_success.window_start": window_start,
                "etl_job.fetch_data_success.window_end": window_end,
                "etl_job.fetch_data_success.file_path": file_path,
            },
        )

        return file_path, rows_count

    def _write_rows(self, file_path: str) -> None:
        """Write rows from file to ClickHouse.

        Loads data from JSONL file and inserts into ClickHouse. If this fails,
        no progress is updated, allowing job to retry the same window on next run.
        Empty file is handled gracefully (no-op).

        Args:
            file_path: Path to JSONL file with data in JSONEachRow format

        Raises:
            Exception: If ClickHouse insert fails
        """
        # Check if file is empty (no rows to write)
        if os.path.getsize(file_path) == 0:
            logger.info("No rows to write (empty result from Prometheus)")
            return

        try:
            self._ch.insert_from_file(file_path)
            logger.info(f"Successfully wrote data to ClickHouse from file {file_path}")
        except Exception as exc:
            logger.error(
                "Failed to write rows to ClickHouse",
                extra={
                    "etl_job.write_failed.message": str(exc),
                    "etl_job.write_failed.file_path": file_path,
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
        timestamp_start: int,
        timestamp_end: int,
        timestamp_progress: int,
        window_seconds: int,
        rows_count: int,
    ) -> None:
        """Save progress and batch metrics to ClickHouse.

        Updates job state only after successful data write. This ensures
        progress advances only when data is actually persisted. If this fails,
        progress is not updated, but data is already in ClickHouse, so next
        run will process the same window again (idempotent behavior).

        Uses the same timestamp_start as the initial start record to update
        the same row in ReplacingMergeTree (ORDER BY timestamp_start).

        Args:
            timestamp_start: Job start timestamp (same as initial start record)
            timestamp_end: Job completion timestamp
            timestamp_progress: New progress timestamp (old + window_size)
            window_seconds: Size of processed window (for monitoring)
            rows_count: Number of rows processed (for monitoring)

        Raises:
            Exception: If ClickHouse save fails
        """
        try:
            self._ch.save_state(
                timestamp_start=timestamp_start,
                timestamp_end=timestamp_end,
                timestamp_progress=timestamp_progress,
                batch_window_seconds=window_seconds,
                batch_rows=rows_count,
            )
            logger.info(
                f"Metrics updated: progress={int(timestamp_progress)}, "
                f"rows={rows_count}, window={window_seconds}s"
            )
        except Exception as exc:
            logger.error(
                "Failed to save metrics after successful batch",
                extra={
                    "etl_job.save_metrics_failed.message": str(exc),
                },
            )
            raise
