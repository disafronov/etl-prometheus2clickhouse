#!/usr/bin/env python3
"""
ClickHouse client wrapper for batch inserts.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from urllib.parse import urlparse

import clickhouse_connect
import requests

from config import ClickHouseConfig
from logging_config import getLogger

logger = getLogger(__name__)


class ClickHouseClient:
    """Client for inserting rows into ClickHouse.

    Provides batch insert functionality for efficient data loading. Uses
    clickhouse-connect library for HTTP-based inserts.
    """

    def __init__(self, config: ClickHouseConfig) -> None:
        """Initialize ClickHouse client.

        Creates connection to ClickHouse HTTP interface. Connection is
        established immediately during initialization to fail fast on
        configuration or connectivity errors.

        Args:
            config: ClickHouse connection configuration

        Raises:
            Exception: If connection initialization fails
        """
        self._config = config
        self._table_metrics = config.table_metrics
        self._table_etl = config.table_etl

        # Store URL and auth for HTTP streaming inserts
        self._http_url = config.url
        self._http_auth = None
        if config.user:
            # Password is normalized by ClickHouseConfig validator:
            # if user is specified but password is None, it's converted to "".
            # Empty string "" is different from None for HTTP Basic Auth.
            self._http_auth = (config.user, config.password or "")
        self._http_verify = not config.insecure

        try:
            # Parse URL to extract host, port, and scheme
            # clickhouse-connect 0.10.0+ requires host and port instead of url
            parsed_url = urlparse(config.url)
            host = parsed_url.hostname
            if host is None:
                raise ValueError(f"Invalid URL: missing hostname in {config.url}")

            port = parsed_url.port
            if port is None:
                # Default ports based on scheme
                port = 8443 if parsed_url.scheme == "https" else 8123

            # Determine if connection should be secure (HTTPS)
            secure = parsed_url.scheme == "https"

            # Type ignore: clickhouse_connect.get_client accepts None and empty string
            # for password but mypy types are too strict. Empty string "" is a valid
            # password value (different from None which means no password).
            # verify parameter controls TLS certificate verification
            # When insecure=True, verify=False disables certificate validation
            self._client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=config.user,
                password=config.password,  # type: ignore[arg-type]
                secure=secure,
                connect_timeout=config.connect_timeout,
                send_receive_timeout=config.send_receive_timeout,
                verify=not config.insecure,
            )
        except Exception as exc:
            logger.error(
                f"Failed to create ClickHouse client: {type(exc).__name__}: {exc}",
                extra={
                    "clickhouse_client.connection_failed.error": str(exc),
                    "clickhouse_client.connection_failed.url": config.url,
                },
            )
            raise

    def _to_unix_timestamp(self, value: datetime | int | None) -> int | None:
        """Convert ClickHouse DateTime value to Unix timestamp (int).

        ClickHouse returns datetime objects for DateTime columns. This method
        converts them to Unix timestamps (int) for consistency with API.

        Important: clickhouse-connect returns naive datetime objects (without
        timezone info). In Python, calling .timestamp() on a naive datetime
        interprets it as local system time, not UTC. However, ClickHouse stores
        DateTime values as UTC internally. This method explicitly converts
        naive datetime to UTC-aware datetime before calling .timestamp() to
        ensure correct conversion.

        Args:
            value: datetime object, int, or None from ClickHouse

        Returns:
            Unix timestamp as int, or None if input is None
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            # Ensure datetime is in UTC before converting to timestamp
            # clickhouse-connect may return datetime in local timezone
            if value.tzinfo is None:
                # Naive datetime - assume UTC (ClickHouse DateTime is
                # timezone-agnostic, stored as UTC)
                value = value.replace(tzinfo=timezone.utc)
            elif value.tzinfo != timezone.utc:
                # Convert to UTC
                value = value.astimezone(timezone.utc)
            return int(value.timestamp())
        # Fallback for int (shouldn't happen with DateTime, but just in case)
        return int(value)

    def insert_from_file(self, file_path: str) -> None:
        """Insert rows from TSV file into configured table.

        Streams data from file directly to ClickHouse via HTTP POST.
        This method is memory-efficient as it streams file without
        loading entire file into memory.

        Args:
            file_path: Path to TSV file with data in TabSeparated format.
                Each row must have columns in order: timestamp, name,
                labels.key (ClickHouse array format), labels.value (ClickHouse
                array format), and value. The id field is MATERIALIZED and always
                auto-generated. Arrays are in ClickHouse format: ['a','b'].
                No header row is included.

        Raises:
            FileNotFoundError: If file doesn't exist
            Exception: If ClickHouse insert operation fails
        """
        if not os.path.exists(file_path):
            error_msg = f"File not found: {file_path}"
            logger.error(
                error_msg,
                extra={
                    "clickhouse_client.insert_from_file_failed.error": error_msg,
                    "clickhouse_client.insert_from_file_failed.file_name": (
                        os.path.basename(file_path)
                    ),
                    "clickhouse_client.insert_from_file_failed.table": (
                        self._table_metrics
                    ),
                },
            )
            raise FileNotFoundError(error_msg)

        # Check if file is empty (no rows to insert)
        # This avoids unnecessary HTTP POST request for empty files
        if os.path.getsize(file_path) == 0:
            logger.info("No rows to insert (empty file)")
            return

        self._validate_table_name(self._table_metrics, "table_metrics")

        # Use HTTP POST with streaming file upload (like curl --data-binary)
        # TabSeparated format supports arrays and Nested structures via HTTP
        # This is memory-efficient as it streams file directly to ClickHouse
        # without loading entire file into memory
        try:
            # Construct query parameter for INSERT statement
            # id field is MATERIALIZED, so it's always auto-generated and
            # cannot be overridden.
            # TabSeparated format expects columns in table order:
            # timestamp, name, labels.key[], labels.value[], value
            # Arrays are in ClickHouse format: ['a','b']
            query = f"INSERT INTO {self._table_metrics} FORMAT TabSeparated"

            # Stream file directly to ClickHouse HTTP interface
            # Using requests.post with file object enables streaming
            # (like curl --data-binary @file.tsv)
            with open(file_path, "rb") as f:
                response = requests.post(
                    self._http_url,
                    params={"query": query},
                    data=f,
                    auth=self._http_auth,
                    verify=self._http_verify,
                    timeout=self._config.send_receive_timeout,
                )
                response.raise_for_status()
        except Exception as exc:
            error_msg = (
                f"Failed to insert from file into ClickHouse via HTTP streaming: "
                f"{type(exc).__name__}: {exc}"
            )
            logger.error(
                error_msg,
                extra={
                    "clickhouse_client.insert_from_file_failed.error": str(exc),
                    "clickhouse_client.insert_from_file_failed.file_name": (
                        os.path.basename(file_path)
                    ),
                    "clickhouse_client.insert_from_file_failed.table": (
                        self._table_metrics
                    ),
                },
            )
            raise

    def get_state(self) -> dict[str, int | None]:
        """Read latest ETL state from ClickHouse.

        Reads the most recent state record ordered by timestamp_start DESC.
        Since ORDER BY key is (timestamp_start), records with same timestamp_start
        are merged by ReplacingMergeTree. We get the latest completed run by
        filtering for records with timestamp_progress IS NOT NULL,
        timestamp_end IS NOT NULL, and timestamp_end > timestamp_start
        (completed records must have both and end must be after start).

        Returns:
            Dictionary with keys: timestamp_progress, timestamp_start,
            timestamp_end, batch_window_seconds, batch_rows, batch_skipped_count.
            All values are int or None if not set. Timestamps are Unix timestamps
            in seconds. batch_window_seconds, batch_rows, and batch_skipped_count
            are integers.

        Raises:
            Exception: If query fails
        """
        try:
            # Table name comes from configuration, not user input.
            # ClickHouse doesn't support parameterized table names in queries,
            # so we validate the table name format and use f-string.
            self._validate_table_name(self._table_etl, "table_etl")
            # FINAL is used to get the latest merged version from ReplacingMergeTree.
            # This is safe for performance because only one ETL job instance writes
            # to this table, so the table size remains small.
            query = f"""
                SELECT
                    timestamp_start,
                    timestamp_end,
                    timestamp_progress,
                    batch_window_seconds,
                    batch_rows,
                    batch_skipped_count
                FROM {self._table_etl} FINAL
                WHERE timestamp_progress IS NOT NULL
                  AND timestamp_end IS NOT NULL
                  AND timestamp_end > timestamp_start
                ORDER BY timestamp_start DESC
                LIMIT 1
            """  # nosec B608
            result = self._client.query(query)

            if not result.result_rows:
                return {
                    "timestamp_progress": None,
                    "timestamp_start": None,
                    "timestamp_end": None,
                    "batch_window_seconds": None,
                    "batch_rows": None,
                    "batch_skipped_count": None,
                }

            row = result.result_rows[0]
            return {
                "timestamp_start": self._to_unix_timestamp(row[0]),
                "timestamp_end": self._to_unix_timestamp(row[1]),
                "timestamp_progress": self._to_unix_timestamp(row[2]),
                "batch_window_seconds": int(row[3]) if row[3] is not None else None,
                "batch_rows": int(row[4]) if row[4] is not None else None,
                "batch_skipped_count": int(row[5]) if row[5] is not None else None,
            }
        except Exception as exc:
            error_msg = (
                f"Failed to read state from ClickHouse: {type(exc).__name__}: {exc}"
            )
            logger.error(
                error_msg,
                extra={
                    "clickhouse_client.get_state_failed.error": str(exc),
                    "clickhouse_client.get_state_failed.table": self._table_etl,
                },
            )
            raise

    @staticmethod
    def _validate_table_name(table_name: str, field_name: str) -> None:
        """Validate table name format to prevent SQL injection.

        ClickHouse doesn't support parameterized table names in queries,
        so we validate the table name format. Only alphanumeric characters,
        underscores, and dots are allowed (standard ClickHouse identifier format).
        Format must be either 'table' or 'database.table', both parts must be
        non-empty and valid identifiers.

        Args:
            table_name: Table name to validate
            field_name: Name of field for error message

        Raises:
            ValueError: If table name contains invalid characters or format
        """
        if not table_name or not table_name.strip():
            raise ValueError(f"Invalid {field_name}: table name cannot be empty")

        parts = table_name.split(".")
        if len(parts) > 2:
            raise ValueError(
                f"Invalid {field_name} format: {table_name} (too many dots)"
            )

        for part in parts:
            if not part or not part.strip():
                raise ValueError(
                    f"Invalid {field_name} format: {table_name} (empty part)"
                )
            if not all(c.isalnum() or c == "_" for c in part):
                raise ValueError(
                    f"Invalid {field_name} format: {table_name} "
                    f"(invalid characters in part: {part})"
                )

    def save_state(
        self,
        timestamp_progress: int | None = None,
        timestamp_start: int | None = None,
        timestamp_end: int | None = None,
        batch_window_seconds: int | None = None,
        batch_rows: int | None = None,
        batch_skipped_count: int | None = None,
    ) -> None:
        """Save ETL state in ClickHouse.

        Always uses INSERT to save state. ReplacingMergeTree handles
        deduplication based on ORDER BY key (timestamp_start). When reading state,
        FINAL is used to get the latest merged version after automatic merges.

        This approach works because:
        1. ClickHouse doesn't allow updating key columns via ALTER TABLE UPDATE
        2. ReplacingMergeTree automatically merges rows with same ORDER BY key
        3. FINAL ensures we read the latest version after merges
        4. When _mark_start() creates record with timestamp_start, and
           _save_state_after_success() adds other fields with same timestamp_start,
           they will be merged into single record

        All fields are optional - only provided fields are saved.
        The id field is MATERIALIZED and always auto-generated, cannot be
        provided. Fields are saved in table order: id (auto-generated),
        timestamp_start, timestamp_end, timestamp_progress,
        batch_window_seconds, batch_rows, batch_skipped_count.

        Args:
            timestamp_progress: Progress timestamp (Unix timestamp in seconds, int)
            timestamp_start: Start timestamp (Unix timestamp in seconds, int)
            timestamp_end: End timestamp (Unix timestamp in seconds, int)
            batch_window_seconds: Window size in seconds
            batch_rows: Number of rows processed
            batch_skipped_count: Number of value pairs skipped due to format errors

        Raises:
            Exception: If insert fails
        """
        try:
            # Always use INSERT instead of UPDATE.
            # ReplacingMergeTree handles deduplication based on ORDER BY key
            # (timestamp_start). When reading state, FINAL is used to get the
            # latest merged version. This approach works because:
            # 1. ClickHouse doesn't allow updating key columns via ALTER TABLE UPDATE
            # 2. ReplacingMergeTree automatically merges rows with same ORDER BY key
            # 3. FINAL ensures we read the latest version after merges
            columns = []
            values = []

            # Save fields in table order:
            # timestamp_start, timestamp_end, timestamp_progress, ...
            # Convert Unix timestamps to datetime objects for DateTime columns.
            # clickhouse-connect requires datetime objects for DateTime columns,
            # not raw Unix timestamps (int). Passing int directly may cause
            # incorrect interpretation (e.g., as days since epoch instead of seconds).
            if timestamp_start is not None:
                columns.append("timestamp_start")
                values.append(datetime.fromtimestamp(timestamp_start, tz=timezone.utc))
            if timestamp_end is not None:
                columns.append("timestamp_end")
                values.append(datetime.fromtimestamp(timestamp_end, tz=timezone.utc))
            if timestamp_progress is not None:
                columns.append("timestamp_progress")
                values.append(
                    datetime.fromtimestamp(timestamp_progress, tz=timezone.utc)
                )
            if batch_window_seconds is not None:
                columns.append("batch_window_seconds")
                # Type ignore: values list contains mixed types (datetime for
                # timestamps, int for batch fields), which is correct for
                # clickhouse-connect insert
                values.append(batch_window_seconds)  # type: ignore[arg-type]
            if batch_rows is not None:
                columns.append("batch_rows")
                # Type ignore: values list contains mixed types (datetime for
                # timestamps, int for batch fields), which is correct for
                # clickhouse-connect insert
                values.append(batch_rows)  # type: ignore[arg-type]
            if batch_skipped_count is not None:
                columns.append("batch_skipped_count")
                # Type ignore: values list contains mixed types (datetime for
                # timestamps, int for batch fields), which is correct for
                # clickhouse-connect insert
                values.append(batch_skipped_count)  # type: ignore[arg-type]

            if not columns:
                return  # Nothing to insert

            self._validate_table_name(self._table_etl, "table_etl")

            self._client.insert(
                self._table_etl,
                [values],
                column_names=columns,
            )
        except Exception as exc:
            logger.error(
                "Failed to save state to ClickHouse",
                extra={
                    "clickhouse_client.save_state_failed.error": str(exc),
                    "clickhouse_client.save_state_failed.table": self._table_etl,
                },
            )
            raise

    def _get_running_job_timestamps(self, use_final: bool = True) -> list[int]:
        """Get list of timestamp_start values for running jobs.

        A running job is one that has an open record (timestamp_end IS NULL)
        without a corresponding closed record (timestamp_end IS NOT NULL
        AND timestamp_end > timestamp_start) for the same timestamp_start.

        This handles ReplacingMergeTree unmerged records correctly:
        - If only open record exists → job is running
        - If both open and closed records exist → job is completed (merge pending)
        - If only closed record exists → job is completed
        - If no records → no job running

        Args:
            use_final: If True, use FINAL to get merged view. If False, check
                raw records (useful for atomic operations where FINAL is expensive).

        Returns:
            List of timestamp_start values for running jobs

        Raises:
            Exception: If query fails
        """
        self._validate_table_name(self._table_etl, "table_etl")

        # Use subquery to apply FINAL when needed, as ClickHouse doesn't support
        # "FROM table FINAL AS alias" syntax directly
        if use_final:
            table_expr = f"(SELECT * FROM {self._table_etl} FINAL)"  # nosec B608
        else:
            table_expr = self._table_etl

        # Use LEFT JOIN instead of NOT EXISTS to avoid correlated subquery issues
        # in ClickHouse (especially Altinity builds) when using FINAL in subqueries.
        # The condition "closed.timestamp_start IS NULL" ensures we only get open
        # records without a corresponding closed record.
        query = f"""
            SELECT DISTINCT open.timestamp_start
            FROM {table_expr} AS open
            LEFT JOIN {table_expr} AS closed
              ON closed.timestamp_start = open.timestamp_start
              AND closed.timestamp_end IS NOT NULL
              AND closed.timestamp_end > closed.timestamp_start
            WHERE open.timestamp_start IS NOT NULL
              AND open.timestamp_end IS NULL
              AND closed.timestamp_start IS NULL
        """  # nosec B608

        result = self._client.query(query)
        # Filter out None values (shouldn't happen
        # due to WHERE timestamp_start IS NOT NULL)
        return [
            ts
            for row in result.result_rows
            if (ts := self._to_unix_timestamp(row[0])) is not None
        ]

    def has_running_job(self) -> bool:
        """Check if there is a running job in the ETL table.

        A running job is defined as having an open record (timestamp_end IS NULL)
        without a corresponding closed record (timestamp_end IS NOT NULL
        AND timestamp_end > timestamp_start) for the same timestamp_start.

        Returns:
            True if a running job exists, False otherwise

        Raises:
            Exception: If query fails
        """
        try:
            running_timestamps = self._get_running_job_timestamps(use_final=True)
            return len(running_timestamps) > 0
        except Exception as exc:
            error_msg = f"Failed to check for running job: {type(exc).__name__}: {exc}"
            logger.error(
                error_msg,
                extra={
                    "clickhouse_client.has_running_job_failed.error": str(exc),
                    "clickhouse_client.has_running_job_failed.table": self._table_etl,
                },
            )
            raise

    def try_mark_start(self, timestamp_start: int) -> bool:
        """Atomically try to mark job start if no other job is running.

        Uses INSERT with subquery to atomically check condition and insert.
        Only one job can successfully mark start at a time.

        Checks for running jobs using the same logic as has_running_job():
        - If there's an open record without a closed record → job is running,
          block start
        - If there's both open and closed records → job is completed, allow start
        - If there's no open record → no job running, allow start

        Note: Uses toDateTime() to explicitly convert Unix timestamp (int) to
        DateTime type in ClickHouse. After insertion, verifies success by
        checking running jobs without FINAL (new inserts are visible immediately
        before merge in ReplacingMergeTree).

        Args:
            timestamp_start: Unix timestamp when job started (int, seconds since epoch)

        Returns:
            True if start was marked successfully, False if another job is running

        Raises:
            Exception: If query fails
        """
        try:
            self._validate_table_name(self._table_etl, "table_etl")

            # Atomic INSERT with condition: only insert if no running job exists.
            # A running job is one that has an open record (timestamp_end IS NULL)
            # without a corresponding closed record (timestamp_end IS NOT NULL
            # AND timestamp_end > timestamp_start).
            # Use LEFT JOIN with COUNT to avoid correlated subquery issues
            # in ClickHouse 25.3+ (especially Altinity builds).
            # We don't use FINAL in subquery because:
            # 1. New inserts are visible immediately (before merge)
            # 2. FINAL is expensive and not needed for this check
            # 3. We need to check both open and closed records separately
            query = f"""
                INSERT INTO {self._table_etl} (timestamp_start)
                SELECT toDateTime({timestamp_start})
                WHERE (
                    SELECT COUNT(*)
                    FROM {self._table_etl} AS open
                    LEFT JOIN {self._table_etl} AS closed
                      ON closed.timestamp_start = open.timestamp_start
                      AND closed.timestamp_end IS NOT NULL
                      AND closed.timestamp_end > closed.timestamp_start
                    WHERE open.timestamp_start IS NOT NULL
                      AND open.timestamp_end IS NULL
                      AND closed.timestamp_start IS NULL
                ) = 0
            """  # nosec B608

            self._client.query(query)

            # Verify insertion succeeded by checking if we're the only running job
            # Don't use FINAL here - new inserts are visible immediately (before merge)
            running_timestamps = self._get_running_job_timestamps(use_final=False)

            # Check if we're the only running job and it's our timestamp_start
            if (
                len(running_timestamps) == 1
                and running_timestamps[0] == timestamp_start
            ):
                return True

            # Insert failed or verification failed
            return False

        except Exception as exc:
            error_msg = (
                f"Failed to atomically mark job start: {type(exc).__name__}: {exc}"
            )
            logger.error(
                error_msg,
                extra={
                    "clickhouse_client.try_mark_start_failed.error": str(exc),
                    "clickhouse_client.try_mark_start_failed.timestamp_start": (
                        timestamp_start
                    ),
                    "clickhouse_client.try_mark_start_failed.table": self._table_etl,
                },
            )
            raise
