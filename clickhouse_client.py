#!/usr/bin/env python3
"""
ClickHouse client wrapper for batch inserts.
"""

from __future__ import annotations

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
        self._table = config.table
        self._table_state = config.table_state

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
                    "clickhouse_client.insert_failed.table": self._table,
                    "clickhouse_client.insert_failed.rows_count": len(rows),
                },
            )
            raise

        try:
            self._client.insert(
                self._table,
                data,
                column_names=list(columns),
            )
        except Exception as exc:
            error_details = f"{type(exc).__name__}: {exc}"
            logger.error(
                f"Failed to insert rows into ClickHouse: {error_details}",
                extra={
                    "clickhouse_client.insert_failed.error": str(exc),
                    "clickhouse_client.insert_failed.table": self._table,
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
        import os

        if not os.path.exists(file_path):
            error_msg = f"File not found: {file_path}"
            logger.error(
                error_msg,
                extra={
                    "clickhouse_client.insert_from_file_failed.error": error_msg,
                    "clickhouse_client.insert_from_file_failed.file_path": file_path,
                    "clickhouse_client.insert_from_file_failed.table": self._table,
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
                    self._table,
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

            rows: list[dict[str, Any]] = []
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    row = json.loads(line)
                    rows.append(row)

            if rows:
                self.insert_rows(rows)
        except Exception as exc:
            error_details = f"{type(exc).__name__}: {exc}"
            logger.error(
                f"Failed to insert from file into ClickHouse: {error_details}",
                extra={
                    "clickhouse_client.insert_from_file_failed.error": str(exc),
                    "clickhouse_client.insert_from_file_failed.file_path": file_path,
                    "clickhouse_client.insert_from_file_failed.table": self._table,
                },
            )
            raise

    def get_state(self) -> dict[str, int | None]:
        """Read latest ETL state from ClickHouse.

        Reads the most recent state record ordered by updated_at.
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
            if not all(c.isalnum() or c in ("_", ".") for c in self._table_state):
                raise ValueError(f"Invalid table name format: {self._table_state}")
            query = f"""
                SELECT
                    timestamp_progress,
                    timestamp_start,
                    timestamp_end,
                    batch_window_seconds,
                    batch_rows
                FROM {self._table_state}
                ORDER BY updated_at DESC
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
                    "clickhouse_client.get_state_failed.table": self._table_state,
                },
            )
            raise

    def save_state(
        self,
        timestamp_progress: int | None = None,
        timestamp_start: int | None = None,
        timestamp_end: int | None = None,
        batch_window_seconds: int | None = None,
        batch_rows: int | None = None,
    ) -> None:
        """Save ETL state to ClickHouse.

        Inserts new state record. ReplacingMergeTree will handle deduplication
        if needed. All fields are optional - only provided fields are saved.

        Args:
            timestamp_progress: Progress timestamp (Unix timestamp in seconds, int)
            timestamp_start: Start timestamp (Unix timestamp in seconds, int)
            timestamp_end: End timestamp (Unix timestamp in seconds, int)
            batch_window_seconds: Window size in seconds
            batch_rows: Number of rows processed

        Raises:
            Exception: If insert fails
        """
        try:
            # Build insert with only non-None values
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

            # updated_at is set by DEFAULT now()
            self._client.insert(
                self._table_state,
                [values],
                column_names=columns,
            )
        except Exception as exc:
            logger.error(
                "Failed to save state to ClickHouse",
                extra={
                    "clickhouse_client.save_state_failed.error": str(exc),
                    "clickhouse_client.save_state_failed.table": self._table_state,
                },
            )
            raise
