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
