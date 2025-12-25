"""
Comprehensive tests for PrometheusClient.
"""

from unittest.mock import Mock, patch

import pytest
import requests

from config import PrometheusConfig
from prometheus_client import PrometheusClient


def _make_prometheus_config(**kwargs: object) -> PrometheusConfig:
    """Create PrometheusConfig for tests, disabling .env file reading.

    Args:
        **kwargs: Additional config parameters to override defaults

    Returns:
        PrometheusConfig instance with test defaults
    """
    defaults = {
        "_env_file": [],  # Disable .env file reading in tests
        "user": None,
        "password": None,
        "url": "http://prom:9090",
    }
    defaults.update(kwargs)
    return PrometheusConfig(**defaults)


def test_prometheus_client_init() -> None:
    """Client should be constructed with minimal config."""
    config = _make_prometheus_config()
    client = PrometheusClient(config)
    assert client._base_url == "http://prom:9090"
    assert client._timeout == 10
    assert client._verify is True


def test_prometheus_client_init_with_auth() -> None:
    """Client should use basic auth when user and password are provided."""
    config = _make_prometheus_config(user="testuser", password="testpass")
    client = PrometheusClient(config)
    assert client._auth == ("testuser", "testpass")


def test_prometheus_client_init_with_empty_password() -> None:
    """Client should use basic auth with empty string password when explicitly set."""
    config = _make_prometheus_config(user="testuser", password="")
    client = PrometheusClient(config)
    assert client._auth == ("testuser", "")


def test_prometheus_client_init_with_user_but_no_password() -> None:
    """Client should normalize None password to empty string when user is specified.

    This handles the case when PROMETHEUS_PASSWORD is set to empty string
    in environment variables and env_ignore_empty=True converts it to None.
    HTTP Basic Auth requires explicit authentication even with empty password.
    """
    config = _make_prometheus_config(user="testuser", password=None)
    # Password should be normalized to empty string by validator
    assert config.password == ""
    client = PrometheusClient(config)
    assert client._auth == ("testuser", "")


def test_prometheus_client_init_with_insecure() -> None:
    """Client should disable TLS verification when insecure=True."""
    config = _make_prometheus_config(insecure=True)
    client = PrometheusClient(config)
    assert client._verify is False


@patch("prometheus_client.requests.get")
def test_prometheus_client_query_range_success(mock_get: Mock) -> None:
    """query_range() should return parsed JSON response."""
    config = _make_prometheus_config()
    client = PrometheusClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.url = "http://prom:9090/api/v1/query_range"
    mock_response.json.return_value = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {"__name__": "up"},
                    "values": [[1700000000, "1"]],
                }
            ]
        },
    }
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = client.query_range("up", start=1700000000, end=1700000300, step="300s")

    assert result["status"] == "success"
    assert "data" in result
    mock_get.assert_called_once()
    call_kwargs = mock_get.call_args[1]
    assert call_kwargs["params"]["query"] == "up"
    assert call_kwargs["params"]["start"] == 1700000000
    assert call_kwargs["params"]["end"] == 1700000300
    assert call_kwargs["params"]["step"] == "300s"


@patch("prometheus_client.requests.get")
@patch("prometheus_client.logger")
def test_prometheus_client_query_range_timeout(
    mock_logger: Mock, mock_get: Mock
) -> None:
    """query_range() should log and re-raise Timeout exception."""
    config = _make_prometheus_config()
    client = PrometheusClient(config)

    mock_get.side_effect = requests.Timeout("Request timeout")

    with pytest.raises(requests.Timeout, match="Request timeout"):
        client.query_range("up", start=1700000000, end=1700000300, step="300s")

    # Check that error was logged
    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args
    assert "Prometheus query_range timeout" in call_args[0][0]
    assert call_args[1]["extra"]["prometheus_client.query_range_timeout.error"] == (
        "Request timeout"
    )


@patch("prometheus_client.requests.get")
@patch("prometheus_client.logger")
def test_prometheus_client_query_range_connection_error(
    mock_logger: Mock, mock_get: Mock
) -> None:
    """query_range() should log and re-raise ConnectionError exception."""
    config = _make_prometheus_config()
    client = PrometheusClient(config)

    mock_get.side_effect = requests.ConnectionError("Connection failed")

    with pytest.raises(requests.ConnectionError, match="Connection failed"):
        client.query_range("up", start=1700000000, end=1700000300, step="300s")

    # Check that error was logged
    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args
    assert "Prometheus query_range connection error" in call_args[0][0]
    assert call_args[1]["extra"][
        "prometheus_client.query_range_connection_error.error"
    ] == ("Connection failed")


@patch("prometheus_client.requests.get")
@patch("prometheus_client.logger")
def test_prometheus_client_query_range_request_exception(
    mock_logger: Mock, mock_get: Mock
) -> None:
    """query_range() should log and re-raise RequestException exception."""
    config = _make_prometheus_config()
    client = PrometheusClient(config)

    mock_get.side_effect = requests.RequestException("Request failed")

    with pytest.raises(requests.RequestException, match="Request failed"):
        client.query_range("up", start=1700000000, end=1700000300, step="300s")

    # Check that error was logged
    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args
    assert "Prometheus query_range request failed" in call_args[0][0]
    assert call_args[1]["extra"][
        "prometheus_client.query_range_request_failed.error"
    ] == ("Request failed")
