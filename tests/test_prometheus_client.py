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
def test_prometheus_client_execute_request_without_extra_log_fields(
    mock_get: Mock,
) -> None:
    """_execute_request() should work when extra_log_fields is not provided (None)."""
    config = _make_prometheus_config()
    client = PrometheusClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.url = "http://prom:9090/api/v1/query_range"
    mock_get.return_value = mock_response

    # Call _execute_request directly with None to test the None check
    # This tests the defensive code path in _execute_request
    response = client._execute_request(
        url="http://prom:9090/api/v1/query_range",
        params={"query": "up", "start": 1700000000, "end": 1700000300, "step": "300s"},
        expr="up",
        query_type="query_range",
        extra_log_fields=None,
    )

    # Verify response was returned successfully
    assert response.status_code == 200


@patch("prometheus_client.requests.get")
def test_prometheus_client_query_range_to_file_success(
    mock_get: Mock, tmp_path
) -> None:
    """query_range_to_file() should stream response to file."""
    config = _make_prometheus_config()
    client = PrometheusClient(config)

    # Create mock response with iter_content
    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.url = "http://prom:9090/api/v1/query_range"
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content.return_value = [
        b'{"status":"success","data":{"result":[',
        b'{"metric":{"__name__":"up"},"values":[[1700000000,"1"]]}',
        b"]}}",
    ]
    mock_get.return_value = mock_response

    file_path = tmp_path / "prometheus_response.json"
    client.query_range_to_file(
        "up", start=1700000000, end=1700000300, step="300s", file_path=str(file_path)
    )

    # Verify file was created and contains response
    assert file_path.exists()
    content = file_path.read_bytes()
    assert b"status" in content
    assert b"up" in content
    mock_response.iter_content.assert_called_once_with(chunk_size=8192)


@patch("prometheus_client.requests.get")
@patch("prometheus_client.logger")
def test_prometheus_client_query_range_to_file_http_error(
    mock_logger: Mock, mock_get: Mock, tmp_path
) -> None:
    """query_range_to_file() should raise exception on HTTP error."""
    config = _make_prometheus_config()
    client = PrometheusClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 500
    mock_response.url = "http://prom:9090/api/v1/query_range"
    mock_response.text = "Internal Server Error"
    mock_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
    mock_get.return_value = mock_response

    file_path = tmp_path / "prometheus_response.json"

    with pytest.raises(requests.HTTPError):
        client.query_range_to_file(
            "up",
            start=1700000000,
            end=1700000300,
            step="300s",
            file_path=str(file_path),
        )

    # File should not be created on error
    assert not file_path.exists()


@patch("prometheus_client.requests.get")
def test_prometheus_client_query_range_to_file_write_error(
    mock_get: Mock, tmp_path
) -> None:
    """query_range_to_file() should raise exception on file write error."""
    config = _make_prometheus_config()
    client = PrometheusClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.url = "http://prom:9090/api/v1/query_range"
    mock_response.raise_for_status.return_value = None
    mock_response.iter_content.return_value = [b"test data"]
    mock_get.return_value = mock_response

    # Use invalid path to cause write error
    file_path = "/nonexistent/directory/file.json"

    with pytest.raises(OSError):
        client.query_range_to_file(
            "up",
            start=1700000000,
            end=1700000300,
            step="300s",
            file_path=file_path,
        )
