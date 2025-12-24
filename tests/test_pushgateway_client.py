"""
Comprehensive tests for PushGatewayClient.
"""

from unittest.mock import Mock, patch

import pytest
import requests

from config import PushGatewayConfig
from pushgateway_client import PushGatewayClient


def test_pushgateway_client_init() -> None:
    """Client should be constructed with minimal config."""
    config = PushGatewayConfig(
        url="http://pg:9091",
        job="test_job",
        instance="test_instance",
    )
    client = PushGatewayClient(config)
    assert client._base_url == "http://pg:9091"
    assert client._job == "test_job"
    assert client._instance == "test_instance"
    assert client._timeout == 10
    assert client._auth is None
    assert client._headers == {}
    assert client._verify is True


def test_pushgateway_client_init_with_token() -> None:
    """Client should use Bearer token when token is provided."""
    config = PushGatewayConfig(
        url="http://pg:9091",
        job="test_job",
        instance="test_instance",
        token="test_token",
    )
    client = PushGatewayClient(config)
    assert client._headers["Authorization"] == "Bearer test_token"
    assert client._auth is None
    assert client._verify is True


def test_pushgateway_client_init_with_basic_auth() -> None:
    """Client should use basic auth when user and password are provided."""
    config = PushGatewayConfig(
        url="http://pg:9091",
        job="test_job",
        instance="test_instance",
        user="testuser",
        password="testpass",
    )
    client = PushGatewayClient(config)
    assert client._auth == ("testuser", "testpass")
    assert "Authorization" not in client._headers
    assert client._verify is True


def test_pushgateway_client_init_with_insecure() -> None:
    """Client should disable TLS verification when insecure=True."""
    config = PushGatewayConfig(
        url="http://pg:9091",
        job="test_job",
        instance="test_instance",
        insecure=True,
    )
    client = PushGatewayClient(config)
    assert client._verify is False


@patch("pushgateway_client.requests.post")
def test_pushgateway_client_push_start_success(mock_post: Mock) -> None:
    """push_start() should push TimestampStart metric successfully."""
    config = PushGatewayConfig(
        url="http://pg:9091",
        job="test_job",
        instance="test_instance",
    )
    client = PushGatewayClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response

    client.push_start(1700000000.0)

    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args[1]
    assert call_kwargs["timeout"] == 10
    assert call_kwargs["verify"] is True
    assert "/metrics/job/test_job/instance/test_instance" in mock_post.call_args[0][0]
    body = call_kwargs["data"]
    assert "etl_timestamp_start" in body
    assert "1700000000.0" in body


@patch("pushgateway_client.requests.post")
def test_pushgateway_client_push_success_metrics(mock_post: Mock) -> None:
    """push_success() should push all required metrics."""
    config = PushGatewayConfig(
        url="http://pg:9091",
        job="test_job",
        instance="test_instance",
    )
    client = PushGatewayClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response

    client.push_success(
        timestamp_end=1700000300.0,
        timestamp_progress=1700000600.0,
        window_seconds=300,
        rows_count=42,
    )

    mock_post.assert_called_once()
    body = mock_post.call_args[1]["data"]
    assert "etl_timestamp_end" in body
    assert "etl_timestamp_progress" in body
    assert "etl_batch_window_seconds" in body
    assert "etl_batch_rows" in body
    assert "1700000300.0" in body
    assert "1700000600.0" in body
    assert "300" in body
    assert "42" in body


@patch("pushgateway_client.requests.post")
def test_pushgateway_client_push_start_http_error(mock_post: Mock) -> None:
    """push_start() should raise exception on HTTP error."""
    config = PushGatewayConfig(
        url="http://pg:9091",
        job="test_job",
        instance="test_instance",
    )
    client = PushGatewayClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = requests.HTTPError("Server error")
    mock_post.return_value = mock_response

    with pytest.raises(requests.HTTPError, match="Server error"):
        client.push_start(1700000000.0)


@patch("pushgateway_client.requests.post")
def test_pushgateway_client_push_success_http_error(mock_post: Mock) -> None:
    """push_success() should raise exception on HTTP error."""
    config = PushGatewayConfig(
        url="http://pg:9091",
        job="test_job",
        instance="test_instance",
    )
    client = PushGatewayClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = requests.HTTPError("Server error")
    mock_post.return_value = mock_response

    with pytest.raises(requests.HTTPError, match="Server error"):
        client.push_success(
            timestamp_end=1700000300.0,
            timestamp_progress=1700000600.0,
            window_seconds=300,
            rows_count=42,
        )


@patch("pushgateway_client.requests.post")
def test_pushgateway_client_push_with_insecure(mock_post: Mock) -> None:
    """push_start() should use verify=False when insecure=True."""
    config = PushGatewayConfig(
        url="http://pg:9091",
        job="test_job",
        instance="test_instance",
        insecure=True,
    )
    client = PushGatewayClient(config)

    mock_response = Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_post.return_value = mock_response

    client.push_start(1700000000.0)

    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args[1]
    assert call_kwargs["verify"] is False
