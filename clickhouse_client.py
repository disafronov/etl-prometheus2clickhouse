#!/usr/bin/env python3
"""
ClickHouse client wrapper for batch inserts.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

import clickhouse_connect

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
            error_details = f"{type(exc).__name__}: {exc}"
            logger.error(
                f"Failed to create ClickHouse client: {error_details}",
                extra={
                    "clickhouse_client.connection_failed.error": str(exc),
                    "clickhouse_client.connection_failed.url": config.url,
                },
            )
            raise

    def insert_rows(self, rows: list[dict[str, Any]]) -> None:
        """Insert rows into configured table in a single batch.

        Performs atomic batch insert for efficiency. Empty list is handled
        gracefully (no-op) to avoid unnecessary database calls. All rows
        must have required keys: timestamp, metric_name, labels, value.

        Args:
            rows: List of row dictionaries to insert

        Raises:
            KeyError: If row is missing required keys
            Exception: If ClickHouse insert operation fails
        """
        if not rows:
            return

        try:
            # Expect rows with keys: timestamp, metric_name, labels, value
            columns = ("timestamp", "metric_name", "labels", "value")
            data = [
                (row["timestamp"], row["metric_name"], row["labels"], row["value"])
                for row in rows
            ]
        except KeyError as exc:
            error_msg = f"Invalid row format for ClickHouse insert: Missing key: {exc}"
            logger.error(
                error_msg,
                extra={
                    "clickhouse_client.insert_failed.error": f"Missing key: {exc}",
                    "clickhouse_client.insert_failed.table": self._table_metrics,
                    "clickhouse_client.insert_failed.rows_count": len(rows),
                },
            )
            raise

        try:
            self._client.insert(
                self._table_metrics,
                data,
                column_names=list(columns),
            )
        except Exception as exc:
            error_details = f"{type(exc).__name__}: {exc}"
            logger.error(
                f"Failed to insert rows into ClickHouse: {error_details}",
                extra={
                    "clickhouse_client.insert_failed.error": str(exc),
                    "clickhouse_client.insert_failed.table": self._table_metrics,
                    "clickhouse_client.insert_failed.rows_count": len(rows),
                },
            )
            raise

    def insert_from_file(self, file_path: str) -> None:
        """Insert rows from JSONL file into configured table.

        Loads data from file in JSONEachRow format and inserts into ClickHouse.
        This method is memory-efficient as it streams data from file without
        loading entire file into memory.

        Args:
            file_path: Path to JSONL file with data in JSONEachRow format.
                Each line must be a JSON object with keys: timestamp, metric_name,
                labels (JSON string), and value.

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
                    "clickhouse_client.insert_from_file_failed.file_path": file_path,
                    "clickhouse_client.insert_from_file_failed.table": (
                        self._table_metrics
                    ),
                },
            )
            raise FileNotFoundError(error_msg)

        try:
            # Use insert_file method if available, otherwise fall back to raw_query
            # clickhouse-connect supports insert_file for streaming file uploads
            # Format: JSONEachRow matches our JSONL format (one JSON object per line)
            # Type ignore: insert_file may not be available in all versions,
            # we catch AttributeError if it's missing
            with open(file_path, "rb") as f:
                self._client.insert_file(  # type: ignore[attr-defined]
                    self._table_metrics,
                    f,
                    column_names=["timestamp", "metric_name", "labels", "value"],
                    format_="JSONEachRow",
                )
        except AttributeError:
            # Fallback: if insert_file is not available, use raw_query with
            # INSERT FROM FILE. This requires file to be accessible by ClickHouse
            # server, which may not work in all deployment scenarios, so we
            # prefer insert_file.
            logger.warning(
                "insert_file method not available, using alternative method",
                extra={
                    "clickhouse_client.insert_from_file_method": "fallback",
                    "clickhouse_client.insert_from_file_failed.file_path": file_path,
                },
            )
            # Read file and insert via standard insert method as fallback
            # This is less efficient but works when insert_file is unavailable
            import json

            file_size = os.path.getsize(file_path)
            rows: list[dict[str, Any]] = []
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    row = json.loads(line)
                    rows.append(row)

            # Warn about performance impact for large files when using fallback
            # Fallback loads entire file into memory, which can be problematic
            # for very large files
            if file_size > 10 * 1024 * 1024:  # 10 MB threshold
                logger.warning(
                    (
                        "Large file detected in fallback mode: file will be loaded "
                        "entirely into memory. This may cause performance issues. "
                        "Consider upgrading clickhouse-connect to use insert_file."
                    ),
                    extra={
                        "clickhouse_client.insert_from_file_fallback_performance.file_path": (  # noqa: E501
                            file_path
                        ),
                        "clickhouse_client.insert_from_file_fallback_performance.file_size_bytes": (  # noqa: E501
                            file_size
                        ),
                        "clickhouse_client.insert_from_file_fallback_performance.rows_count": (  # noqa: E501
                            len(rows)
                        ),
                    },
                )

            if rows:
                self.insert_rows(rows)
        except Exception as exc:
            error_details = f"{type(exc).__name__}: {exc}"
            logger.error(
                f"Failed to insert from file into ClickHouse: {error_details}",
                extra={
                    "clickhouse_client.insert_from_file_failed.error": str(exc),
                    "clickhouse_client.insert_from_file_failed.file_path": file_path,
                    "clickhouse_client.insert_from_file_failed.table": (
                        self._table_metrics
                    ),
                },
            )
            raise

    def get_state(self) -> dict[str, int | None]:
        """Read latest ETL state from ClickHouse.

        Reads the most recent state record ordered by progress, then timestamps.
        Returns None for missing fields to match Prometheus behavior.

        Returns:
            Dictionary with keys: timestamp_progress, timestamp_start,
            timestamp_end, batch_window_seconds, batch_rows.
            Timestamp values are int (Unix timestamp in seconds) or None if not set.

        Raises:
            Exception: If query fails
        """
        try:
            # Table name comes from configuration, not user input.
            # ClickHouse doesn't support parameterized table names in queries,
            # so we validate the table name format and use f-string.
            # Format: database.table or just table
            # (allowed characters: alphanumeric, underscore, dot)
            if not all(c.isalnum() or c in ("_", ".") for c in self._table_etl):
                raise ValueError(f"Invalid table name format: {self._table_etl}")
            # FINAL is used to get the latest merged version from ReplacingMergeTree.
            # This is safe for performance because only one ETL job instance writes
            # to this table, so the table size remains small.
            query = f"""
                SELECT
                    timestamp_progress,
                    timestamp_start,
                    timestamp_end,
                    batch_window_seconds,
                    batch_rows
                FROM {self._table_etl} FINAL
                ORDER BY timestamp_progress DESC NULLS LAST,
                         timestamp_start DESC NULLS LAST,
                         timestamp_end DESC NULLS LAST
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
                }

            row = result.result_rows[0]
            return {
                "timestamp_progress": int(row[0]) if row[0] is not None else None,
                "timestamp_start": int(row[1]) if row[1] is not None else None,
                "timestamp_end": int(row[2]) if row[2] is not None else None,
                "batch_window_seconds": row[3] if row[3] is not None else None,
                "batch_rows": row[4] if row[4] is not None else None,
            }
        except Exception as exc:
            logger.error(
                "Failed to read state from ClickHouse",
                extra={
                    "clickhouse_client.get_state_failed.error": str(exc),
                    "clickhouse_client.get_state_failed.table": self._table_etl,
                },
            )
            raise

    @staticmethod
    def _validate_int_value(value: int | None, field_name: str) -> None:
        """Validate that value is int (not None) before SQL insertion.

        This prevents SQL injection even if types change in future code.
        All values inserted into SQL queries must be validated int to ensure
        they cannot contain SQL injection payloads.

        Args:
            value: Value to validate (must be int, not None)
            field_name: Name of field for error message

        Raises:
            TypeError: If value is not int (None is allowed but not used in SQL)
        """
        if value is not None and not isinstance(value, int):
            raise TypeError(
                f"{field_name} must be int, got {type(value).__name__}: {value}"
            )

    def save_state(
        self,
        timestamp_progress: int | None = None,
        timestamp_start: int | None = None,
        timestamp_end: int | None = None,
        batch_window_seconds: int | None = None,
        batch_rows: int | None = None,
    ) -> None:
        """Save or update ETL state in ClickHouse.

        If timestamp_start is provided and there are other fields to update,
        updates existing record with that timestamp_start using ALTER TABLE UPDATE.
        Otherwise, inserts new record. All fields are optional - only provided
        fields are saved/updated.

        Args:
            timestamp_progress: Progress timestamp (Unix timestamp in seconds, int)
            timestamp_start: Start timestamp (Unix timestamp in seconds, int).
                If provided along with other fields, updates existing record.
            timestamp_end: End timestamp (Unix timestamp in seconds, int)
            batch_window_seconds: Window size in seconds
            batch_rows: Number of rows processed

        Raises:
            Exception: If insert or update fails
        """
        try:
            # Check if we should update existing record
            # Update if timestamp_start is provided AND there are other fields to update
            other_fields = [
                timestamp_progress,
                timestamp_end,
                batch_window_seconds,
                batch_rows,
            ]
            has_other_fields = any(f is not None for f in other_fields)

            if timestamp_start is not None and has_other_fields:
                # Update existing record by timestamp_start
                # has_other_fields guarantees at least one field is not None,
                # so updates will never be empty
                # Validate all values are int before SQL construction to prevent
                # SQL injection. This is critical security measure even though
                # types guarantee int
                self._validate_int_value(timestamp_start, "timestamp_start")
                if timestamp_progress is not None:
                    self._validate_int_value(timestamp_progress, "timestamp_progress")
                if timestamp_end is not None:
                    self._validate_int_value(timestamp_end, "timestamp_end")
                if batch_window_seconds is not None:
                    self._validate_int_value(
                        batch_window_seconds, "batch_window_seconds"
                    )
                if batch_rows is not None:
                    self._validate_int_value(batch_rows, "batch_rows")

                # Now safe to construct SQL - all values are validated int
                updates = []
                if timestamp_progress is not None:
                    updates.append(f"timestamp_progress = {timestamp_progress}")
                if timestamp_end is not None:
                    updates.append(f"timestamp_end = {timestamp_end}")
                if batch_window_seconds is not None:
                    updates.append(f"batch_window_seconds = {batch_window_seconds}")
                if batch_rows is not None:
                    updates.append(f"batch_rows = {batch_rows}")

                # Table name comes from configuration, not user input.
                # ClickHouse doesn't support parameterized table names in queries,
                # so we validate the table name format and use f-string.
                # Format: database.table or just table
                # (allowed characters: alphanumeric, underscore, dot)
                if not all(c.isalnum() or c in ("_", ".") for c in self._table_etl):
                    raise ValueError(f"Invalid table name format: {self._table_etl}")
                query = f"""
                    ALTER TABLE {self._table_etl}
                    UPDATE {', '.join(updates)}
                    WHERE timestamp_start = {timestamp_start}
                """  # nosec B608
                self._client.command(query)
            else:
                # Insert new record
                columns = []
                values = []

                if timestamp_progress is not None:
                    columns.append("timestamp_progress")
                    values.append(timestamp_progress)
                if timestamp_start is not None:
                    columns.append("timestamp_start")
                    values.append(timestamp_start)
                if timestamp_end is not None:
                    columns.append("timestamp_end")
                    values.append(timestamp_end)
                if batch_window_seconds is not None:
                    columns.append("batch_window_seconds")
                    values.append(batch_window_seconds)
                if batch_rows is not None:
                    columns.append("batch_rows")
                    values.append(batch_rows)

                if not columns:
                    return  # Nothing to insert

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
