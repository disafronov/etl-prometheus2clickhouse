"""
Comprehensive tests for ClickHouseClient.
"""

import io
import logging
from unittest.mock import Mock, patch

import pytest

from clickhouse_client import ClickHouseClient
from config import ClickHouseConfig


def _make_clickhouse_config(**kwargs: object) -> ClickHouseConfig:
    """Create ClickHouseConfig for tests, disabling .env file reading.

    Args:
        **kwargs: Additional config parameters to override defaults

    Returns:
        ClickHouseConfig instance with test defaults
    """
    defaults = {
        "_env_file": [],  # Disable .env file reading in tests
        "user": None,
        "password": None,
        "url": "http://ch:8123",
        "table": "db.tbl",
    }
    defaults.update(kwargs)
    return ClickHouseConfig(**defaults)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init(mock_get_client: Mock) -> None:
    """Client should be constructed with minimal config."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)
    assert client._table == "db.tbl"
    assert client._client == mock_client
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username=None,
        password=None,
        secure=False,
        connect_timeout=10,
        send_receive_timeout=300,
        verify=True,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_auth(mock_get_client: Mock) -> None:
    """Client should use auth when user and password are provided."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(user="testuser", password="testpass")
    ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username="testuser",
        password="testpass",
        secure=False,
        connect_timeout=10,
        send_receive_timeout=300,
        verify=True,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_empty_password(mock_get_client: Mock) -> None:
    """Client should pass empty string password when explicitly set."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(user="testuser", password="")
    ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username="testuser",
        password="",  # Empty string should be passed, not None
        secure=False,
        connect_timeout=10,
        send_receive_timeout=300,
        verify=True,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_user_but_no_password(
    mock_get_client: Mock,
) -> None:
    """Client should normalize None password to empty string when user is specified.

    This handles the case when CLICKHOUSE_PASSWORD is set to empty string
    in environment variables and env_ignore_empty=True converts it to None.
    ClickHouse requires explicit authentication even with empty password.
    """
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(user="default", password=None)
    # Password should be normalized to empty string by validator
    assert cfg.password == ""
    ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username="default",
        password="",  # Normalized from None to empty string
        secure=False,
        connect_timeout=10,
        send_receive_timeout=300,
        verify=True,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_custom_timeouts(mock_get_client: Mock) -> None:
    """Client should use custom timeout values when provided."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(connect_timeout=30, send_receive_timeout=600)
    ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username=None,
        password=None,
        secure=False,
        connect_timeout=30,
        send_receive_timeout=600,
        verify=True,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_insecure(mock_get_client: Mock) -> None:
    """Client should disable TLS verification when insecure=True."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(insecure=True)
    ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username=None,
        password=None,
        secure=False,
        connect_timeout=10,
        send_receive_timeout=300,
        verify=False,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_https_url(mock_get_client: Mock) -> None:
    """Client should use HTTPS port 8443 and secure=True for https:// URLs."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(url="https://ch:8443")
    ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8443,
        username=None,
        password=None,
        secure=True,
        connect_timeout=10,
        send_receive_timeout=300,
        verify=True,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_https_url_no_port(mock_get_client: Mock) -> None:
    """Client should default to port 8443 for https:// URLs when port not specified."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(url="https://ch")
    ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8443,
        username=None,
        password=None,
        secure=True,
        connect_timeout=10,
        send_receive_timeout=300,
        verify=True,
    )


def test_clickhouse_client_init_with_invalid_url_missing_hostname() -> None:
    """Client should raise ValueError when URL has no hostname."""
    # URL without hostname (e.g., "http://" or "http://:8123")
    cfg = _make_clickhouse_config(url="http://:8123")

    with pytest.raises(ValueError, match="Invalid URL: missing hostname"):
        ClickHouseClient(cfg)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_connection_error(mock_get_client: Mock) -> None:
    """Client should raise exception on connection failure."""
    mock_get_client.side_effect = Exception("Connection refused")

    cfg = _make_clickhouse_config()

    with pytest.raises(Exception, match="Connection refused"):
        ClickHouseClient(cfg)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_connection_error_logs_details(
    mock_get_client: Mock,
) -> None:
    """Client should log error details when connection fails."""
    mock_get_client.side_effect = Exception("stream closed: EOF")

    # Capture log output
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.ERROR)

    logger = logging.getLogger("clickhouse_client")
    if logger.handlers:
        existing_formatter = logger.handlers[0].formatter
        if existing_formatter:
            handler.setFormatter(existing_formatter)
    logger.addHandler(handler)

    try:
        cfg = _make_clickhouse_config()

        with pytest.raises(Exception, match="stream closed: EOF"):
            ClickHouseClient(cfg)

        # Check that error message contains error details
        output = stream.getvalue()
        assert "Failed to create ClickHouse client" in output
        assert "Exception: stream closed: EOF" in output
    finally:
        logger.removeHandler(handler)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_success(mock_get_client: Mock) -> None:
    """insert_rows() should insert data successfully."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    rows = [
        {
            # ClickHouse DateTime requires integer Unix timestamp
            "timestamp": 1700000000,
            "metric_name": "up",
            "labels": '{"instance":"localhost"}',
            "value": 1.0,
        },
        {
            "timestamp": 1700000300,
            "metric_name": "up",
            "labels": '{"instance":"localhost"}',
            "value": 1.0,
        },
    ]

    client.insert_rows(rows)

    mock_client.insert.assert_called_once()
    call_args = mock_client.insert.call_args
    assert call_args[0][0] == "db.tbl"
    assert len(call_args[0][1]) == 2
    assert call_args[1]["column_names"] == [
        "timestamp",
        "metric_name",
        "labels",
        "value",
    ]


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_empty_list(mock_get_client: Mock) -> None:
    """insert_rows() should do nothing when rows list is empty."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    client.insert_rows([])

    mock_client.insert.assert_not_called()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_missing_key(mock_get_client: Mock) -> None:
    """insert_rows() should raise KeyError when row is missing required key."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    rows = [
        {
            # ClickHouse DateTime requires integer Unix timestamp
            "timestamp": 1700000000,
            "metric_name": "up",
            # Missing "labels" key
            "value": 1.0,
        }
    ]

    with pytest.raises(KeyError):
        client.insert_rows(rows)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_missing_key_logs_details(
    mock_get_client: Mock,
) -> None:
    """insert_rows() should log error details when row is missing required key."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    # Capture log output
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.ERROR)

    logger = logging.getLogger("clickhouse_client")
    if logger.handlers:
        existing_formatter = logger.handlers[0].formatter
        if existing_formatter:
            handler.setFormatter(existing_formatter)
    logger.addHandler(handler)

    try:
        cfg = _make_clickhouse_config()
        client = ClickHouseClient(cfg)

        rows = [
            {
                "timestamp": 1700000000,
                "metric_name": "up",
                # Missing "labels" key
                "value": 1.0,
            }
        ]

        with pytest.raises(KeyError):
            client.insert_rows(rows)

        # Check that error message contains error details
        output = stream.getvalue()
        assert "Invalid row format for ClickHouse insert" in output
        assert "Missing key" in output
    finally:
        logger.removeHandler(handler)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_insert_error(mock_get_client: Mock) -> None:
    """insert_rows() should raise exception when insert fails."""
    mock_client = Mock()
    mock_client.insert.side_effect = Exception("Insert failed")
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    rows = [
        {
            # ClickHouse DateTime requires integer Unix timestamp
            "timestamp": 1700000000,
            "metric_name": "up",
            "labels": '{"instance":"localhost"}',
            "value": 1.0,
        }
    ]

    with pytest.raises(Exception, match="Insert failed"):
        client.insert_rows(rows)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_insert_error_logs_details(
    mock_get_client: Mock,
) -> None:
    """insert_rows() should log error details when insert fails."""
    mock_client = Mock()
    mock_client.insert.side_effect = Exception("Table not found")
    mock_get_client.return_value = mock_client

    # Capture log output
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.ERROR)

    logger = logging.getLogger("clickhouse_client")
    if logger.handlers:
        existing_formatter = logger.handlers[0].formatter
        if existing_formatter:
            handler.setFormatter(existing_formatter)
    logger.addHandler(handler)

    try:
        cfg = _make_clickhouse_config()
        client = ClickHouseClient(cfg)

        rows = [
            {
                "timestamp": 1700000000,
                "metric_name": "up",
                "labels": '{"instance":"localhost"}',
                "value": 1.0,
            }
        ]

        with pytest.raises(Exception, match="Table not found"):
            client.insert_rows(rows)

        # Check that error message contains error details
        output = stream.getvalue()
        assert "Failed to insert rows into ClickHouse" in output
        assert "Exception: Table not found" in output
    finally:
        logger.removeHandler(handler)
