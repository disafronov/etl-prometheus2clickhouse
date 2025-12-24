#!/usr/bin/env python3
"""
PushGateway client for sending ETL state metrics.
"""

from __future__ import annotations

import requests

from config import PushGatewayConfig
from logging_config import getLogger

logger = getLogger(__name__)


class PushGatewayClient:
    """Client for pushing ETL metrics to Prometheus PushGateway.

    PushGateway is write-only: job state metrics are pushed here but never
    read back. State is read from Prometheus (which scrapes PushGateway).
    This design separates write operations from read operations.
    """

    def __init__(self, config: PushGatewayConfig) -> None:
        """Initialize PushGateway client.

        Sets up authentication (Bearer token or Basic auth) and connection
        parameters. Only one auth method is used: token takes precedence
        over basic auth.

        Args:
            config: PushGateway connection configuration
        """
        self._config = config
        self._base_url = config.url.rstrip("/")
        self._job = config.job
        self._instance = config.instance
        self._timeout = config.timeout

        self._auth: tuple[str, str] | None = None
        self._headers: dict[str, str] = {}
        self._verify = not config.insecure

        if config.token:
            self._headers["Authorization"] = f"Bearer {config.token}"
        elif config.user and config.password:
            self._auth = (config.user, config.password)

    def _push(self, metrics_lines: list[str]) -> None:
        """Push raw metric lines to PushGateway.

        Sends metrics in Prometheus text format to PushGateway endpoint.
        Metrics are associated with job and instance labels from config.

        Args:
            metrics_lines: List of metric lines in Prometheus text format

        Raises:
            requests.RequestException: If HTTP request fails
        """
        path = f"/metrics/job/{self._job}/instance/{self._instance}"
        url = f"{self._base_url}{path}"
        body = "\n".join(metrics_lines) + "\n"

        try:
            response = requests.post(
                url,
                data=body,
                timeout=self._timeout,
                auth=self._auth,
                headers=self._headers,
                verify=self._verify,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.error(
                "Failed to push metrics to PushGateway",
                extra={
                    "pushgateway_client.push_failed.error": str(exc),
                    "pushgateway_client.push_failed.url": url,
                },
            )
            raise

    def push_start(self, timestamp_start: float) -> None:
        """Push TimestampStart metric when job starts.

        Atomically marks job start. This must succeed before processing begins
        to prevent concurrent runs. Called early in job execution.

        Args:
            timestamp_start: Unix timestamp when job started

        Raises:
            requests.RequestException: If push fails
        """
        lines = [
            "# TYPE etl_timestamp_start gauge",
            f"etl_timestamp_start {timestamp_start}",
        ]
        self._push(lines)

    def push_success(
        self,
        timestamp_end: float,
        timestamp_progress: float,
        window_seconds: int,
        rows_count: int,
    ) -> None:
        """Push metrics when batch finished successfully.

        Updates job state after successful data processing. Pushes:
        - TimestampEnd: marks job completion
        - TimestampProgress: advances to next window
        - Batch metadata: window size and row count for monitoring

        Called only after successful ClickHouse write to ensure progress
        advances only when data is persisted.

        Args:
            timestamp_end: Job completion timestamp
            timestamp_progress: New progress timestamp
            window_seconds: Size of processed window
            rows_count: Number of rows processed

        Raises:
            requests.RequestException: If push fails
        """
        lines = [
            "# TYPE etl_timestamp_end gauge",
            f"etl_timestamp_end {timestamp_end}",
            "# TYPE etl_timestamp_progress gauge",
            f"etl_timestamp_progress {timestamp_progress}",
            "# TYPE etl_batch_window_seconds gauge",
            f"etl_batch_window_seconds {window_seconds}",
            "# TYPE etl_batch_rows gauge",
            f"etl_batch_rows {rows_count}",
        ]
        self._push(lines)
