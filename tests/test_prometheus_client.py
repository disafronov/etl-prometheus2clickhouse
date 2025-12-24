"""
Comprehensive tests for PrometheusClient.
"""

from unittest.mock import Mock, patch

import pytest
import requests

from config import PrometheusConfig
from prometheus_client import PrometheusClient


def test_prometheus_client_init() -> None:
    """Client should be constructed with minimal config."""
    config = PrometheusConfig(url="http://prom:9090")
    client = PrometheusClient(config)
    assert client._base_url == "http://prom:9090"
    assert client._timeout == 10
    assert client._verify is True


def test_prometheus_client_init_with_auth() -> None:
    """Client should use basic auth when user and password are provided."""
    config = PrometheusConfig(
        url="http://prom:9090",
        user="testuser",
        password="testpass",
    )
    client = PrometheusClient(config)
    assert client._auth == ("testuser", "testpass")


def test_prometheus_client_init_with_insecure() -> None:
    """Client should disable TLS verification when insecure=True."""
    config = PrometheusConfig(
        url="http://prom:9090",
        insecure=True,
    )
    client = PrometheusClient(config)
    assert client._verify is False


@patch("prometheus_client.requests.get")
def test_prometheus_client_query_success(mock_get: Mock) -> None:
    """query() should return parsed JSON response."""
    config = PrometheusConfig(url="http://prom:9090")
    client = PrometheusClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.url = "http://prom:9090/api/v1/query?query=up"
    mock_response.json.return_value = {
        "status": "success",
        "data": {"result": [{"value": [1234567890, "1"]}]},
    }
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = client.query("up")

    assert result["status"] == "success"
    assert "data" in result
    mock_get.assert_called_once()
    call_kwargs = mock_get.call_args[1]
    assert call_kwargs["params"]["query"] == "up"
    assert call_kwargs["timeout"] == 10


@patch("prometheus_client.requests.get")
def test_prometheus_client_query_range_success(mock_get: Mock) -> None:
    """query_range() should return parsed JSON response."""
    config = PrometheusConfig(url="http://prom:9090")
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

    result = client.query_range("up", start=1700000000.0, end=1700000300.0, step="300s")

    assert result["status"] == "success"
    assert "data" in result
    mock_get.assert_called_once()
    call_kwargs = mock_get.call_args[1]
    assert call_kwargs["params"]["query"] == "up"
    assert call_kwargs["params"]["start"] == 1700000000.0
    assert call_kwargs["params"]["end"] == 1700000300.0
    assert call_kwargs["params"]["step"] == "300s"


@patch("prometheus_client.requests.get")
def test_prometheus_client_query_http_error(mock_get: Mock) -> None:
    """query() should raise exception on HTTP error."""
    config = PrometheusConfig(url="http://prom:9090")
    client = PrometheusClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 500
    mock_response.url = "http://prom:9090/api/v1/query?query=up"
    mock_response.raise_for_status.side_effect = requests.HTTPError("Server error")
    mock_get.return_value = mock_response

    with pytest.raises(requests.HTTPError, match="Server error"):
        client.query("up")


@patch("prometheus_client.requests.get")
def test_prometheus_client_query_invalid_json(mock_get: Mock) -> None:
    """query() should raise exception on invalid JSON."""
    config = PrometheusConfig(url="http://prom:9090")
    client = PrometheusClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.url = "http://prom:9090/api/v1/query?query=up"
    mock_response.json.side_effect = ValueError("Invalid JSON")
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    with pytest.raises(ValueError, match="Invalid JSON"):
        client.query("up")


@patch("prometheus_client.requests.get")
def test_prometheus_client_query_non_dict_response(mock_get: Mock) -> None:
    """query() should raise exception when response is not a dict."""
    config = PrometheusConfig(url="http://prom:9090")
    client = PrometheusClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.url = "http://prom:9090/api/v1/query?query=up"
    mock_response.json.return_value = ["not", "a", "dict"]
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    with pytest.raises(ValueError, match="invalid response format"):
        client.query("up")
