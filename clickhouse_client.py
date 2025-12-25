#!/usr/bin/env python3
"""
ClickHouse client wrapper for batch inserts.
"""

from __future__ import annotations

import os
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

    def insert_from_file(self, file_path: str) -> None:
        """Insert rows from JSONL file into configured table.

        Streams data from file directly to ClickHouse via HTTP POST.
        This method is memory-efficient as it streams file without
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

        # Check if file is empty (no rows to insert)
        # This avoids unnecessary HTTP POST request for empty files
        if os.path.getsize(file_path) == 0:
            logger.info("No rows to insert (empty file)")
            return

        self._validate_table_name(self._table_metrics, "table_metrics")

        # Use HTTP POST with streaming file upload
        # This is memory-efficient as it streams file directly to ClickHouse
        # without loading entire file into memory
        try:
            # Construct query parameter for INSERT statement
            query = f"INSERT INTO {self._table_metrics} FORMAT JSONEachRow"

            # Stream file directly to ClickHouse HTTP interface
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
                    "clickhouse_client.insert_from_file_failed.file_path": file_path,
                    "clickhouse_client.insert_from_file_failed.table": (
                        self._table_metrics
                    ),
                },
            )
            raise

    def get_state(self) -> dict[str, int | None]:
        """Read latest ETL state from ClickHouse.

        Reads the most recent state record ordered by timestamp_progress,
        then timestamp_start, then timestamp_end (matching table ORDER BY key).
        Returns None for missing fields to match Prometheus behavior.

        Returns:
            Dictionary with keys: timestamp_progress, timestamp_start,
            timestamp_end, batch_window_seconds, batch_rows.
            All values are int or None if not set. Timestamps are Unix timestamps
            in seconds. batch_window_seconds and batch_rows are integers.

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
                "batch_window_seconds": int(row[3]) if row[3] is not None else None,
                "batch_rows": int(row[4]) if row[4] is not None else None,
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
    ) -> None:
        """Save ETL state in ClickHouse.

        Always uses INSERT to save state. ReplacingMergeTree handles
        deduplication based on ORDER BY key (timestamp_progress, timestamp_start,
        timestamp_end). When reading state, FINAL is used to get the latest
        merged version after automatic merges.

        This approach is used because ClickHouse doesn't allow updating key
        columns via ALTER TABLE UPDATE. ReplacingMergeTree automatically merges
        rows with the same ORDER BY key, keeping the latest version.

        All fields are optional - only provided fields are saved.

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
            # Always use INSERT instead of UPDATE.
            # ReplacingMergeTree handles deduplication based on ORDER BY key
            # (timestamp_progress, timestamp_start, timestamp_end).
            # When reading state, FINAL is used to get the latest merged version.
            # This approach works because:
            # 1. ClickHouse doesn't allow updating key columns via ALTER TABLE UPDATE
            # 2. ReplacingMergeTree automatically merges rows with same ORDER BY key
            # 3. FINAL ensures we read the latest version after merges
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
