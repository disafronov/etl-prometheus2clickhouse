#!/usr/bin/env python3
"""
Core ETL job implementation.

Implements streaming ETL algorithm with three distinct stages:
- Check start condition via TimestampStart/TimestampEnd from ClickHouse.
- Mark start by saving new TimestampStart to ClickHouse.
- Read TimestampProgress from ClickHouse (required, job fails if not found).
- Calculate processing window based on progress and batch window size.
- Extract: Stream Prometheus response to file (prometheus_raw_*.json)
- Transform: Stream parse JSON, process and transform data to ClickHouse format
- Load: Stream processed data to ClickHouse (etl_processed_*.tsv)
- Save updated progress and end timestamps, plus window size and rows count
  to ClickHouse.

All data processing is done in streaming fashion without loading data into
memory, enabling handling of very large Prometheus responses.
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import ijson  # type: ignore[import-untyped]

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

        file_path, rows_count, prom_response_path = self._fetch_data(
            window_start, window_end
        )

        if rows_count > 0:
            try:
                self._ch.insert_from_file(file_path)
                output_filename = os.path.basename(file_path)
                logger.info(
                    f"Successfully wrote data to ClickHouse from {output_filename}, "
                    f"rows: {rows_count}",
                    extra={
                        "etl_job.clickhouse_write_success.file_path": file_path,
                        "etl_job.clickhouse_write_success.rows_count": rows_count,
                    },
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
        # actual_window is the size of data we actually processed
        # We requested from window_start to window_end, but Prometheus returns
        # data only up to current_time. So actual window is from window_start
        # to min(window_end, current_time)
        actual_window = min(window_end, current_time) - window_start

        # Log if window was reduced due to current time limit
        # This is expected behavior: system should not process future data
        # that doesn't exist in Prometheus yet
        if new_progress < expected_progress:
            window_reduced = int(expected_progress - new_progress)
            logger.info(
                f"Progress adjusted to current time (expected behavior): "
                f"expected={format_timestamp_with_utc(int(expected_progress))}, "
                f"actual={format_timestamp_with_utc(int(new_progress))}, "
                f"window_reduced_by={window_reduced}s. "
                f"This is normal - system prevents processing future data."
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
        Window starts at (progress - overlap) to create overlap for data
        reliability, and extends by window_size seconds from the start.
        Overlap shifts the window backward without changing its size.

        Args:
            progress: Current progress timestamp (start of window without overlap, int)

        Returns:
            Tuple of (window_start, window_end) as Unix timestamps (int)
        """
        overlap = self._config.etl.batch_window_overlap_seconds
        window_size = self._config.etl.batch_window_size_seconds
        window_start = progress - overlap  # Shift start backward for overlap
        window_end = window_start + window_size  # End calculated from start
        return window_start, window_end

    def _fetch_data(self, window_start: int, window_end: int) -> tuple[str, int, str]:
        """Fetch data from Prometheus and transform to ClickHouse format.

        Implements streaming ETL pipeline with three stages:
        1. Extract: Stream Prometheus response to file (prometheus_raw_*.json)
        2. Transform: Stream parse JSON, process and transform data
        3. Load: Return processed file path (handled by insert_from_file)

        Queries all metrics using {__name__=~".+"} selector. All processing
        is done in streaming fashion without loading data into memory.

        Args:
            window_start: Start of time range (Unix timestamp, int)
            window_end: End of time range (Unix timestamp, int)

        Returns:
            Tuple of (file_path, rows_count, prom_response_path) where file_path
            is path to TSV file with processed data in TabSeparated format,
            rows_count is number of rows written, and prom_response_path is path
            to the original Prometheus response file

        Raises:
            Exception: If Prometheus query fails, JSON parsing fails, or file
                write fails
        """
        step = f"{self._config.prometheus.query_step_seconds}s"

        # Stage 1 - Extract: Stream Prometheus response to file
        prom_response_fd, prom_response_path = self._create_temp_file(
            prefix="prometheus_raw_", suffix=".json"
        )
        os.close(prom_response_fd)

        try:
            # Stream Prometheus response directly to file
            self._prom.query_range_to_file(
                '{__name__=~".+"}',
                start=window_start,
                end=window_end,
                step=step,
                file_path=prom_response_path,
            )
        except Exception as exc:
            # Clean up file if request failed
            self._cleanup_temp_file(prom_response_path)
            logger.error(
                "Failed to fetch data from Prometheus",
                extra={
                    "etl_job.fetch_failed.message": str(exc),
                    "etl_job.fetch_failed.prom_response_path": prom_response_path,
                },
            )
            raise

        # Get size of Prometheus response file after successful download
        prom_response_file_size = os.path.getsize(prom_response_path)
        prom_response_filename = os.path.basename(prom_response_path)
        logger.info(
            f"Downloaded Prometheus response: {prom_response_filename}, "
            f"size: {prom_response_file_size} bytes",
            extra={
                "etl_job.prometheus_download_success.prom_response_path": (
                    prom_response_path
                ),
                "etl_job.prometheus_download_success.prom_response_file_size": (
                    prom_response_file_size
                ),
            },
        )

        # Stage 2 - Transform: Stream parse JSON and process data
        output_fd, output_file_path = self._create_temp_file(
            prefix="etl_processed_", suffix=".tsv"
        )

        rows_count = 0
        series_count = 0
        try:
            with (
                open(prom_response_path, "rb") as input_f,
                os.fdopen(output_fd, "w", encoding="utf-8", newline="") as output_f,
            ):
                # Stream parse JSON array items
                parser = ijson.items(input_f, "data.result.item")

                for series in parser:
                    series_count += 1
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
                                    "etl_job.invalid_value_pair.name": metric_name,
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

                        # Prepare labels as Nested structure (arrays of keys and values)
                        # ClickHouse Nested(key, value) expects two arrays:
                        # labels.key = ['key1', 'key2', ...]
                        # labels.value = ['value1', 'value2', ...]
                        # Sort keys for consistent ORDER BY comparison.
                        # ORDER BY uses arrayStringConcat(arraySort(labels.key), ',')
                        # so sorting here ensures consistency with ClickHouse
                        # comparison.
                        labels_sorted = dict(sorted(labels.items()))

                        # Convert to Nested format: separate arrays for keys and values
                        labels_keys = list(labels_sorted.keys())
                        labels_values = list(labels_sorted.values())

                        # Format arrays for ClickHouse TabSeparated format
                        # According to ClickHouse documentation:
                        # - Arrays are written as ['a','b'] for strings
                        # - Special characters in strings must be escaped: \ -> \\,
                        #   ' -> \', \t -> \t, \n -> \n
                        labels_keys_str = self._format_clickhouse_array(labels_keys)
                        labels_values_str = self._format_clickhouse_array(labels_values)

                        # Escape string values for TabSeparated format
                        # TabSeparated requires escaping: \ -> \\, \t -> \t, \n -> \n
                        metric_name_escaped = self._escape_tabseparated_chars(
                            metric_name
                        )

                        # Write TabSeparated row: columns separated by \t, rows by \n
                        # Column order: timestamp, name, labels.key[], labels.value[],
                        # value
                        output_f.write(
                            f"{ts}\t{metric_name_escaped}\t{labels_keys_str}\t"
                            f"{labels_values_str}\t{value}\n"
                        )
                        rows_count += 1

        except Exception as exc:
            # Clean up both files on error
            self._cleanup_temp_file(prom_response_path)
            self._cleanup_temp_file(output_file_path)
            logger.error(
                "Failed to transform data from Prometheus response",
                extra={
                    "etl_job.fetch_write_failed.message": str(exc),
                    "etl_job.fetch_write_failed.prom_response_path": (
                        prom_response_path
                    ),
                    "etl_job.fetch_write_failed.output_file_path": output_file_path,
                },
            )
            raise
        finally:
            # Always clean up Prometheus response file after processing
            self._cleanup_temp_file(prom_response_path)

        if rows_count == 0:
            logger.warning(
                "No metrics found in Prometheus for the specified time window",
                extra={
                    "etl_job.fetch_data_empty_result.window_start": window_start,
                    "etl_job.fetch_data_empty_result.window_end": window_end,
                    "etl_job.fetch_data_empty_result.step": step,
                },
            )

        # Log transformation complete with all file information
        # This is logged inside transformation method as requested
        output_filename = os.path.basename(output_file_path)
        output_file_size = os.path.getsize(output_file_path)
        prom_response_filename = os.path.basename(prom_response_path)
        logger.info(
            f"Transformed file ready for ClickHouse: {output_filename}, "
            f"size: {output_file_size} bytes, rows: {rows_count}, "
            f"series: {series_count}",
            extra={
                "etl_job.transformation_complete.file_path": output_file_path,
                "etl_job.transformation_complete.output_file_size": (output_file_size),
                "etl_job.transformation_complete.rows_count": rows_count,
                "etl_job.transformation_complete.series_count": series_count,
                "etl_job.transformation_complete.prom_response_filename": (
                    prom_response_filename
                ),
            },
        )

        return output_file_path, rows_count, prom_response_path

    def _create_temp_file(
        self, prefix: str = "etl_batch_", suffix: str = ".tsv"
    ) -> tuple[int, str]:
        """Create temporary file for ETL data.

        Creates a temporary file in the configured temp directory with explicit
        prefix and suffix for clear identification. Ensures the directory exists
        before creating the file.

        Args:
            prefix: File prefix for explicit naming (default: "etl_batch_")
            suffix: File suffix/extension (default: ".tsv")

        Returns:
            Tuple of (file_descriptor, file_path) where file_descriptor can be
            used with os.fdopen() and file_path is the absolute path to the file
        """
        temp_dir = Path(self._config.etl.temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        fd, file_path = tempfile.mkstemp(suffix=suffix, dir=temp_dir, prefix=prefix)
        return fd, file_path

    @staticmethod
    def _escape_tabseparated_chars(value: str) -> str:
        """Escape basic TabSeparated characters: backslash, tab, newline.

        According to ClickHouse documentation, TabSeparated format requires
        escaping of special characters:
        - Backslash: backslash -> double backslash
        - Tab: tab character -> escaped tab
        - Newline: newline character -> escaped newline

        Args:
            value: String value to escape

        Returns:
            Escaped string with basic TabSeparated characters escaped
        """
        # Escape in order: backslash first, then tab, then newline
        # This ensures we don't double-escape already escaped sequences
        return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")

    @staticmethod
    def _format_clickhouse_array(arr: list[str]) -> str:
        """Format Python list as ClickHouse array string for TabSeparated format.

        According to ClickHouse documentation, arrays in TabSeparated format
        are written as ['a','b'] for strings. String elements must have
        special characters escaped:
        - Backslash: backslash -> double backslash
        - Single quote: single quote -> escaped single quote
        - Tab: tab character -> escaped tab
        - Newline: newline character -> escaped newline

        Args:
            arr: List of strings to format as ClickHouse array

        Returns:
            Formatted array string (e.g., "['a','b']" or "[]" for empty array)
        """
        if not arr:
            return "[]"

        # Escape each element: basic TabSeparated chars, then single quote
        escaped = []
        for elem in arr:
            elem_escaped = EtlJob._escape_tabseparated_chars(elem).replace("'", "\\'")
            escaped.append(f"'{elem_escaped}'")
        return "[" + ",".join(escaped) + "]"

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
