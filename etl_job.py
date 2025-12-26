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
from logging_config import format_timestamp_with_utc, getLogger
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
            RuntimeError: If job cannot start (previous job running or error
                checking state).
            RuntimeError: If job start cannot be marked (error saving state).
            ValueError: If TimestampProgress is not found in ClickHouse.
            Exception: If any step fails (fetch, write, or save state).
        """
        if not self._check_can_start():
            raise RuntimeError(
                "Job cannot start: previous job is still running or "
                "error checking state"
            )

        logger.info("Job can start, beginning ETL cycle")

        timestamp_start = int(time.time())
        if not self._mark_start(timestamp_start):
            raise RuntimeError(
                "Job cannot start: failed to mark job start in ClickHouse"
            )

        logger.info(
            f"Batch window size: {self._config.etl.batch_window_size_seconds}s, "
            f"batch window overlap: {self._config.etl.batch_window_overlap_seconds}s"
        )

        progress = self._load_progress()
        window_start, window_end = self._calc_window(progress)

        logger.info(
            f"Processing window: {format_timestamp_with_utc(window_start)} - "
            f"{format_timestamp_with_utc(window_end)}"
        )

        file_path, rows_count = self._fetch_data(window_start, window_end)
        if rows_count > 0:
            try:
                self._ch.insert_from_file(file_path)
                logger.info(
                    f"Successfully wrote data to ClickHouse from file {file_path}"
                )
            except Exception as exc:
                logger.error(
                    "Failed to write rows to ClickHouse",
                    extra={
                        "etl_job.write_failed.message": str(exc),
                        "etl_job.write_failed.file_path": file_path,
                    },
                )
                raise
            finally:
                # Always clean up temporary file, even if write fails
                self._cleanup_temp_file(file_path)
        else:
            # No data to insert, clean up empty file
            self._cleanup_temp_file(file_path)
            logger.info("No data to insert, skipping ClickHouse write")

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
                f"Progress limited by current time: "
                f"expected={format_timestamp_with_utc(int(expected_progress))}, "
                f"actual={format_timestamp_with_utc(int(new_progress))}, "
                f"window_reduced_by={window_reduced}s"
            )

        # Ensure timestamp_end is always greater than timestamp_start.
        # TimestampEnd must never equal TimestampStart.
        timestamp_end = max(current_time, timestamp_start + 1)

        self._save_state_after_success(
            timestamp_start=timestamp_start,
            timestamp_end=timestamp_end,
            timestamp_progress=new_progress,
            window_seconds=int(actual_window),
            rows_count=rows_count,
        )

    def _read_state_field(self, field_name: str) -> int | None:
        """Read single state field from ClickHouse.

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

        Prevents concurrent job execution by checking for running jobs:
        - Looks for any record with timestamp_start but no timestamp_end
        - If found, previous job is still running, block start
        - If not found, allow start

        Returns:
            True if job can start, False otherwise
        """
        try:
            if self._ch.has_running_job():
                logger.warning(
                    "Previous job is still running",
                    extra={
                        "etl_job.check_start_failed.message": (
                            "Found running job: timestamp_start exists but "
                            "timestamp_end is missing"
                        ),
                    },
                )
                return False

            # No running job found - allow start
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
        """Atomically mark job start if no other job is running.

        Uses INSERT with subquery to atomically check condition and insert.
        Only one job can successfully mark start at a time.

        Args:
            timestamp_start: Unix timestamp when job started (int, seconds since epoch)

        Returns:
            True if start was marked successfully, False if another job is running
        """
        try:
            if self._ch.try_mark_start(timestamp_start):
                logger.info(
                    f"Job start marked atomically at "
                    f"{format_timestamp_with_utc(timestamp_start)}"
                )
                return True
            else:
                logger.warning(
                    "Failed to atomically mark job start - another job may be running",
                    extra={
                        "etl_job.mark_start_failed.message": (
                            "Atomic insert failed or verification failed - "
                            "another job may be running"
                        ),
                    },
                )
                return False
        except Exception as exc:
            logger.error(
                "Failed to atomically mark job start",
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

        State validation is handled by get_state(), which filters for valid
        completed records (timestamp_progress IS NOT NULL AND timestamp_end IS NOT NULL
        AND timestamp_end > timestamp_start). If get_state() returns a record with
        timestamp_progress, it's already valid.

        Returns:
            Current progress timestamp as Unix timestamp (int, seconds since epoch)

        Raises:
            ValueError: If TimestampProgress is not found in ClickHouse.
            Exception: If reading from ClickHouse fails.
        """
        try:
            progress = self._read_state_field("timestamp_progress")
            if progress is not None:
                # get_state() already filters for valid completed records:
                # timestamp_progress IS NOT NULL AND timestamp_end IS NOT NULL
                # AND timestamp_end > timestamp_start.
                # If get_state() returned a record with timestamp_progress,
                # it's already valid. No additional validation needed.
                logger.info(
                    f"Loaded progress timestamp: {format_timestamp_with_utc(progress)}"
                )
                return progress
        except ValueError:
            # Re-raise ValueError as-is (state validation errors)
            raise
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
        batch_window_size_seconds. The end point is also shifted by overlap to
        maintain the configured window size.

        Args:
            progress: Current progress timestamp (start of window without overlap, int)

        Returns:
            Tuple of (window_start, window_end) as Unix timestamps (int)
        """
        overlap = self._config.etl.batch_window_overlap_seconds
        window_size = self._config.etl.batch_window_size_seconds
        window_start = progress - overlap
        window_end = (
            window_start + window_size
        )  # Maintain window_size regardless of overlap
        return window_start, window_end

    def _fetch_data(self, window_start: int, window_end: int) -> tuple[str, int]:
        """Fetch data from Prometheus for given window and write to JSONL file.

        Queries all metrics using {__name__=~".+"} selector to export everything
        available in Prometheus. Writes data to temporary JSONL file in streaming
        fashion to minimize memory usage. Each line contains a JSON object with
        timestamp, name, labels (as JSON string), and value.

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
            fd, file_path = self._create_temp_file()
            os.close(fd)
            return file_path, 0

        # Create temporary file for streaming write
        fd, file_path = self._create_temp_file()

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
                                    "etl_job.invalid_value_pair.name": (metric_name),
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
                            "name": metric_name,
                            "labels": labels_json,
                            "value": value,
                        }
                        f.write(json.dumps(row, separators=(",", ":")) + "\n")
                        rows_count += 1
        except Exception as exc:
            # Clean up file on error
            self._cleanup_temp_file(file_path)
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

    def _create_temp_file(self) -> tuple[int, str]:
        """Create temporary file for ETL batch data.

        Creates a temporary JSONL file in the configured temp directory.
        Ensures the directory exists before creating the file.

        Returns:
            Tuple of (file_descriptor, file_path) where file_descriptor can be
            used with os.fdopen() and file_path is the absolute path to the file
        """
        temp_dir = Path(self._config.etl.temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        fd, file_path = tempfile.mkstemp(
            suffix=".jsonl", dir=temp_dir, prefix="etl_batch_"
        )
        return fd, file_path

    @staticmethod
    def _cleanup_temp_file(file_path: str) -> None:
        """Clean up temporary file, ignoring errors.

        Removes temporary file if it exists. All errors during cleanup are
        silently ignored to prevent cleanup errors from masking original errors.

        Args:
            file_path: Path to temporary file to remove
        """
        try:
            os.unlink(file_path)
        except Exception:  # nosec B110
            # Ignore cleanup errors (file may not exist or already deleted)
            pass

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

    def _save_state_after_success(
        self,
        timestamp_start: int,
        timestamp_end: int,
        timestamp_progress: int,
        window_seconds: int,
        rows_count: int,
    ) -> None:
        """Save progress and batch state to ClickHouse.

        Saves job state only after successful data write. This ensures
        progress advances only when data is actually persisted. If this fails,
        progress is not updated, but data is already in ClickHouse, so next
        run will process the same window again (idempotent behavior).

        Inserts a new record with all state fields. Since ORDER BY key is
        timestamp_start, this record will be merged with the record created
        at start (with same timestamp_start) by ReplacingMergeTree, keeping
        the latest version with all fields populated.

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
                f"State saved: "
                f"progress={format_timestamp_with_utc(timestamp_progress)}, "
                f"rows={rows_count}, window={window_seconds}s"
            )
        except Exception as exc:
            logger.error(
                "Failed to save state after successful batch",
                extra={
                    "etl_job.save_state_failed.message": str(exc),
                },
            )
            raise
