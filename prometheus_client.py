#!/usr/bin/env python3
"""
Prometheus client for executing query and query_range requests.
"""

from __future__ import annotations

from typing import Any

import requests

from config import PrometheusConfig
from logging_config import getLogger

logger = getLogger(__name__)


class PrometheusClient:
    """Client for interacting with Prometheus-compatible HTTP API.

    Supports both Prometheus and Mimir (Prometheus-compatible). Used for:
    - Reading job state metrics (etl_timestamp_*)
    - Fetching metric data via query_range for processing
    """

    def __init__(self, config: PrometheusConfig) -> None:
        """Initialize Prometheus client.

        Sets up authentication and connection parameters.

        Args:
            config: Prometheus connection configuration
        """
        self._config = config
        self._base_url = config.url.rstrip("/")
        self._timeout = config.timeout

        self._auth = None
        # Password is normalized by PrometheusConfig validator: if user is
        # specified but password is None, it's converted to empty string "".
        # Empty string "" is different from None for HTTP Basic Auth.
        # After normalization, password will be either explicitly set or "" (not None).
        if config.user is not None and config.password is not None:
            self._auth = (config.user, config.password)

        self._verify = not config.insecure

    def _handle_response(
        self, response: requests.Response, expression: str
    ) -> dict[str, Any]:
        """Validate Prometheus HTTP response and extract JSON body.

        Ensures response is valid JSON and has expected structure. Raises
        exceptions with detailed logging for debugging.

        Args:
            response: HTTP response from Prometheus API
            expression: Query expression that was executed (for error context)

        Returns:
            Parsed JSON response as dictionary

        Raises:
            requests.RequestException: If HTTP status indicates error
            ValueError: If response is not valid JSON or has wrong structure
        """
        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.error(
                "Prometheus query failed",
                extra={
                    "prometheus_client.query_failed.error": str(exc),
                    "prometheus_client.query_failed.expression": expression,
                    "prometheus_client.query_failed.url": response.url,
                    "prometheus_client.query_failed.status_code": response.status_code,
                },
            )
            raise

        try:
            data = response.json()
        except ValueError as exc:
            logger.error(
                "Prometheus returned invalid JSON",
                extra={
                    "prometheus_client.invalid_response.expression": expression,
                    "prometheus_client.invalid_response.message": str(exc),
                },
            )
            raise

        if not isinstance(data, dict):
            logger.error(
                "Prometheus returned non-dict JSON",
                extra={
                    "prometheus_client.invalid_response.expression": expression,
                    "prometheus_client.invalid_response.message": (
                        "Response root is not a dict"
                    ),
                },
            )
            raise ValueError("Prometheus returned invalid response format")

        return data

    def query(self, expr: str) -> dict[str, Any]:
        """Execute instant query.

        Performs Prometheus instant query (single point in time). Used for
        reading job state metrics like etl_timestamp_start, etl_timestamp_end,
        and etl_timestamp_progress.

        Args:
            expr: PromQL expression to execute

        Returns:
            Prometheus API response dictionary

        Raises:
            requests.RequestException: If HTTP request fails
            ValueError: If response is invalid
        """
        url = f"{self._base_url}/api/v1/query"
        response = requests.get(
            url,
            params={"query": expr},
            timeout=self._timeout,
            auth=self._auth,
            verify=self._verify,
        )
        return self._handle_response(response, expr)

    def query_range(
        self, expr: str, start: float, end: float, step: str
    ) -> dict[str, Any]:
        """Execute range query.

        Performs Prometheus range query (time series over time range). Used
        for fetching metric data for processing. Step parameter controls
        resolution of returned data points.

        Args:
            expr: PromQL expression to execute
            start: Start timestamp (Unix timestamp)
            end: End timestamp (Unix timestamp)
            step: Resolution step (e.g., "300s", "1d")

        Returns:
            Prometheus API response dictionary with time series data

        Raises:
            requests.RequestException: If HTTP request fails
            ValueError: If response is invalid
        """
        url = f"{self._base_url}/api/v1/query_range"
        # requests.get accepts float values in params dict, so we use str | float union
        params: dict[str, str | float] = {
            "query": expr,
            "start": start,
            "end": end,
            "step": step,
        }
        response = requests.get(
            url,
            params=params,
            timeout=self._timeout,
            auth=self._auth,
            verify=self._verify,
        )
        return self._handle_response(response, expr)
