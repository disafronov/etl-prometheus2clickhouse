#!/usr/bin/env python3
"""
Prometheus client for executing query_range requests.
"""

from __future__ import annotations

import os
from typing import Any

import requests

from config import PrometheusConfig
from logging_config import getLogger

logger = getLogger(__name__)


class PrometheusClient:
    """Client for interacting with Prometheus-compatible HTTP API.

    Supports both Prometheus and Mimir (Prometheus-compatible). Used for
    fetching metric data via query_range for processing.
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
        if config.user:
            # Password is normalized by PrometheusConfig validator:
            # if user is specified but password is None, it's converted to "".
            # Empty string "" is different from None for HTTP Basic Auth.
            self._auth = (config.user, config.password or "")

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
        with structured logging. Used by query_range_to_file() method.

        Args:
            url: Full URL to Prometheus API endpoint
            params: Query parameters for the request
            expr: PromQL expression (for error context)
            query_type: Type of query ("query_range") for log prefixes
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

    def query_range_to_file(
        self, expr: str, start: int, end: int, step: str, file_path: str
    ) -> None:
        """Execute range query and stream response to file.

        Streams response body directly to file without loading into memory.
        This is memory-efficient for large Prometheus responses.

        Args:
            expr: PromQL expression to execute
            start: Start timestamp (Unix timestamp in seconds, int)
            end: End timestamp (Unix timestamp in seconds, int)
            step: Resolution step (e.g., "300s", "1d")
            file_path: Path to file where response will be saved

        Raises:
            requests.RequestException: If HTTP request fails
            OSError: If file write fails
        """
        url = f"{self._base_url}/api/v1/query_range"
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

        # Validate HTTP status before streaming
        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            # Try to get response body for better error diagnostics
            response_text = None
            try:  # pragma: no cover
                response_text = response.text[:1000]  # pragma: no cover
            except Exception:  # nosec B110  # pragma: no cover
                # Response body may not be readable in all error scenarios
                # This is intentional defensive code
                pass  # pragma: no cover

            logger.error(
                "Prometheus query failed",
                extra={
                    "prometheus_client.query_failed.error": str(exc),
                    "prometheus_client.query_failed.error_type": type(exc).__name__,
                    "prometheus_client.query_failed.expression": expr,
                    "prometheus_client.query_failed.url": response.url,
                    "prometheus_client.query_failed.status_code": response.status_code,
                    "prometheus_client.query_failed.response_preview": response_text,
                },
            )
            raise

        # Stream response body directly to file
        try:
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # pragma: no branch
                        # iter_content may yield empty chunks, skip them
                        f.write(chunk)
        except OSError as exc:
            logger.error(
                "Failed to write Prometheus response to file",
                extra={
                    "prometheus_client.query_range_to_file_failed.error": str(exc),
                    "prometheus_client.query_range_to_file_failed.file_name": (
                        os.path.basename(file_path)
                    ),
                    "prometheus_client.query_range_to_file_failed.expression": expr,
                },
            )
            raise
