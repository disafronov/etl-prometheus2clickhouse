#!/usr/bin/env python3
"""
ClickHouse client wrapper for batch inserts.
"""

from __future__ import annotations

from typing import Any

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
            # Type ignore: clickhouse_connect.get_client accepts None for password
            # but mypy types are too strict
            # verify parameter controls TLS certificate verification
            # When insecure=True, verify=False disables certificate validation
            self._client = clickhouse_connect.get_client(
                url=config.url,
                username=config.user,
                password=config.password,  # type: ignore[arg-type]
                connect_timeout=config.connect_timeout,
                send_receive_timeout=config.send_receive_timeout,
                verify=not config.insecure,
            )
        except Exception as exc:
            logger.error(
                "Failed to create ClickHouse client",
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
            logger.error(
                "Invalid row format for ClickHouse insert",
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
            logger.error(
                "Failed to insert rows into ClickHouse",
                extra={
                    "clickhouse_client.insert_failed.error": str(exc),
                    "clickhouse_client.insert_failed.table": self._table,
                    "clickhouse_client.insert_failed.rows_count": len(rows),
                },
            )
            raise
