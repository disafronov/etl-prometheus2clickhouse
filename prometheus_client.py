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

    def _execute_request(
        self,
        url: str,
        params: dict[str, str | int],
        expr: str,
        query_type: str,
        extra_log_fields: dict[str, Any] | None = None,
    ) -> requests.Response:
        """Execute HTTP GET request to Prometheus API with error handling.

        Handles common exceptions (Timeout, ConnectionError, RequestException)
        with structured logging. Used by both query() and query_range() methods.

        Args:
            url: Full URL to Prometheus API endpoint
            params: Query parameters for the request
            expr: PromQL expression (for error context)
            query_type: Type of query ("query" or "query_range") for log prefixes
            extra_log_fields: Optional additional fields for logging
                (e.g., step, window_seconds)

        Returns:
            HTTP response object

        Raises:
            requests.Timeout: If request times out
            requests.ConnectionError: If connection fails
            requests.RequestException: If request fails for other reasons
        """
        if extra_log_fields is None:
            extra_log_fields = {}

        try:
            response = requests.get(
                url,
                params=params,
                timeout=self._timeout,
                auth=self._auth,
                verify=self._verify,
            )
        except requests.Timeout as exc:
            log_extra = {
                f"prometheus_client.{query_type}_timeout.error": str(exc),
                f"prometheus_client.{query_type}_timeout.expression": expr,
                f"prometheus_client.{query_type}_timeout.url": url,
                f"prometheus_client.{query_type}_timeout.timeout": self._timeout,
            }
            log_extra.update(extra_log_fields)
            logger.error(
                f"Prometheus {query_type} timeout",
                extra=log_extra,
            )
            raise
        except requests.ConnectionError as exc:
            logger.error(
                f"Prometheus {query_type} connection error",
                extra={
                    f"prometheus_client.{query_type}_connection_error.error": str(exc),
                    f"prometheus_client.{query_type}_connection_error.expression": expr,
                    f"prometheus_client.{query_type}_connection_error.url": url,
                },
            )
            raise
        except requests.RequestException as exc:
            logger.error(
                f"Prometheus {query_type} request failed",
                extra={
                    f"prometheus_client.{query_type}_request_failed.error": str(exc),
                    f"prometheus_client.{query_type}_request_failed.error_type": type(
                        exc
                    ).__name__,
                    f"prometheus_client.{query_type}_request_failed.expression": expr,
                    f"prometheus_client.{query_type}_request_failed.url": url,
                },
            )
            raise

        return response

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
            # Try to get response body for better error diagnostics
            # Limit to first 1000 chars to avoid logging huge responses
            response_text = None
            try:
                response_text = response.text[:1000]
            except Exception:  # nosec B110
                # Ignore errors when reading response body - this is intentional
                # as response may not be readable in all error scenarios
                pass

            logger.error(
                "Prometheus query failed",
                extra={
                    "prometheus_client.query_failed.error": str(exc),
                    "prometheus_client.query_failed.error_type": type(exc).__name__,
                    "prometheus_client.query_failed.expression": expression,
                    "prometheus_client.query_failed.url": response.url,
                    "prometheus_client.query_failed.status_code": response.status_code,
                    "prometheus_client.query_failed.response_preview": response_text,
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
        response = self._execute_request(
            url=url,
            params={"query": expr},
            expr=expr,
            query_type="query",
        )
        return self._handle_response(response, expr)

    def query_range(self, expr: str, start: int, end: int, step: str) -> dict[str, Any]:
        """Execute range query.

        Performs Prometheus range query (time series over time range). Used
        for fetching metric data for processing. Step parameter controls
        resolution of returned data points.

        Args:
            expr: PromQL expression to execute
            start: Start timestamp (Unix timestamp in seconds, int)
            end: End timestamp (Unix timestamp in seconds, int)
            step: Resolution step (e.g., "300s", "1d")

        Returns:
            Prometheus API response dictionary with time series data

        Raises:
            requests.RequestException: If HTTP request fails
            ValueError: If response is invalid
        """
        url = f"{self._base_url}/api/v1/query_range"
        # requests.get accepts int values in params dict
        params: dict[str, str | int] = {
            "query": expr,
            "start": start,
            "end": end,
            "step": step,
        }
        response = self._execute_request(
            url=url,
            params=params,
            expr=expr,
            query_type="query_range",
            extra_log_fields={
                "prometheus_client.query_range_timeout.window_seconds": int(
                    end - start
                ),
                "prometheus_client.query_range_timeout.step": step,
            },
        )
        return self._handle_response(response, expr)
