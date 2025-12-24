"""
Comprehensive tests for ClickHouseClient.
"""

from unittest.mock import Mock, patch

import pytest

from clickhouse_client import ClickHouseClient
from config import ClickHouseConfig


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init(mock_get_client: Mock) -> None:
    """Client should be constructed with minimal config."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = ClickHouseConfig(
        url="http://ch:8123",
        table="db.tbl",
    )
    client = ClickHouseClient(cfg)
    assert client._table == "db.tbl"
    assert client._client == mock_client
    mock_get_client.assert_called_once_with(
        url="http://ch:8123",
        username=None,
        password=None,
        connect_timeout=10,
        send_receive_timeout=300,
        verify=True,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_auth(mock_get_client: Mock) -> None:
    """Client should use auth when user and password are provided."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = ClickHouseConfig(
        url="http://ch:8123",
        table="db.tbl",
        user="testuser",
        password="testpass",
    )
    ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        url="http://ch:8123",
        username="testuser",
        password="testpass",
        connect_timeout=10,
        send_receive_timeout=300,
        verify=True,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_custom_timeouts(mock_get_client: Mock) -> None:
    """Client should use custom timeout values when provided."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = ClickHouseConfig(
        url="http://ch:8123",
        table="db.tbl",
        connect_timeout=30,
        send_receive_timeout=600,
    )
    ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        url="http://ch:8123",
        username=None,
        password=None,
        connect_timeout=30,
        send_receive_timeout=600,
        verify=True,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_insecure(mock_get_client: Mock) -> None:
    """Client should disable TLS verification when insecure=True."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = ClickHouseConfig(
        url="http://ch:8123",
        table="db.tbl",
        insecure=True,
    )
    ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        url="http://ch:8123",
        username=None,
        password=None,
        connect_timeout=10,
        send_receive_timeout=300,
        verify=False,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_connection_error(mock_get_client: Mock) -> None:
    """Client should raise exception on connection failure."""
    mock_get_client.side_effect = Exception("Connection refused")

    cfg = ClickHouseConfig(
        url="http://ch:8123",
        table="db.tbl",
    )

    with pytest.raises(Exception, match="Connection refused"):
        ClickHouseClient(cfg)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_success(mock_get_client: Mock) -> None:
    """insert_rows() should insert data successfully."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = ClickHouseConfig(
        url="http://ch:8123",
        table="db.tbl",
    )
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

    cfg = ClickHouseConfig(
        url="http://ch:8123",
        table="db.tbl",
    )
    client = ClickHouseClient(cfg)

    client.insert_rows([])

    mock_client.insert.assert_not_called()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_missing_key(mock_get_client: Mock) -> None:
    """insert_rows() should raise KeyError when row is missing required key."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = ClickHouseConfig(
        url="http://ch:8123",
        table="db.tbl",
    )
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
def test_clickhouse_client_insert_rows_insert_error(mock_get_client: Mock) -> None:
    """insert_rows() should raise exception when insert fails."""
    mock_client = Mock()
    mock_client.insert.side_effect = Exception("Insert failed")
    mock_get_client.return_value = mock_client

    cfg = ClickHouseConfig(
        url="http://ch:8123",
        table="db.tbl",
    )
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
