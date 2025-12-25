"""
Comprehensive tests for ClickHouseClient.
"""

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
        verify=True,
        connect_timeout=10,
        send_receive_timeout=300,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_auth(mock_get_client: Mock) -> None:
    """Client should be constructed with authentication."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(user="user", password="pass")
    _ = ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username="user",
        password="pass",
        secure=False,
        verify=False,
        connect_timeout=10,
        send_receive_timeout=300,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_empty_password(mock_get_client: Mock) -> None:
    """Client should handle empty password string."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(user="user", password="")
    _ = ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username="user",
        password="",
        secure=False,
        verify=False,
        connect_timeout=10,
        send_receive_timeout=300,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_user_but_no_password(
    mock_get_client: Mock,
) -> None:
    """Client should handle user without password."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(user="user", password=None)
    _ = ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username="user",
        password=None,
        secure=False,
        verify=False,
        connect_timeout=10,
        send_receive_timeout=300,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_custom_timeouts(mock_get_client: Mock) -> None:
    """Client should use custom timeout values."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(connect_timeout=5, send_receive_timeout=60)
    _ = ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username=None,
        password=None,
        secure=False,
        verify=False,
        connect_timeout=5,
        send_receive_timeout=60,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_insecure(mock_get_client: Mock) -> None:
    """Client should handle insecure flag."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(insecure=True)
    _ = ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username=None,
        password=None,
        secure=False,
        verify=False,
        connect_timeout=10,
        send_receive_timeout=300,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_https_url(mock_get_client: Mock) -> None:
    """Client should parse HTTPS URL correctly."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(url="https://ch:8443")
    _ = ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8443,
        username=None,
        password=None,
        secure=True,
        verify=True,
        connect_timeout=10,
        send_receive_timeout=300,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_https_url_no_port(
    mock_get_client: Mock,
) -> None:
    """Client should use default HTTPS port 443."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(url="https://ch")
    _ = ClickHouseClient(cfg)
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8443,
        username=None,
        password=None,
        secure=True,
        verify=True,
        connect_timeout=10,
        send_receive_timeout=300,
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_with_invalid_url_missing_hostname(
    mock_get_client: Mock,
) -> None:
    """Client should raise ValueError for invalid URL."""
    cfg = _make_clickhouse_config(url="http://")
    with pytest.raises(ValueError, match="Invalid URL: missing hostname"):
        ClickHouseClient(cfg)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_connection_error(mock_get_client: Mock) -> None:
    """Client should log and re-raise connection errors."""
    mock_get_client.side_effect = Exception("Connection failed")

    cfg = _make_clickhouse_config()
    with pytest.raises(Exception, match="Connection failed"):
        ClickHouseClient(cfg)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_init_connection_error_logs_details(
    mock_get_client: Mock, caplog
) -> None:
    """Client should log connection error details."""
    mock_get_client.side_effect = Exception("Connection failed")

    cfg = _make_clickhouse_config()
    with pytest.raises(Exception):
        ClickHouseClient(cfg)

    assert "Failed to connect to ClickHouse" in caplog.text
    assert "Connection failed" in caplog.text


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_success(mock_get_client: Mock) -> None:
    """insert_rows() should insert data successfully."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    rows = [
        {"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0},
        {"timestamp": 1234567900, "metric_name": "up", "labels": "{}", "value": 1.0},
    ]

    client.insert_rows(rows)

    mock_client.insert.assert_called_once_with(
        "db.tbl",
        [(1234567890, "up", "{}", 1.0), (1234567900, "up", "{}", 1.0)],
        column_names=["timestamp", "metric_name", "labels", "value"],
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_empty_list(mock_get_client: Mock) -> None:
    """insert_rows() should handle empty list."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    client.insert_rows([])

    # Empty list should not call insert
    mock_client.insert.assert_not_called()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_missing_key(mock_get_client: Mock) -> None:
    """insert_rows() should raise KeyError for missing required key."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    rows = [{"timestamp": 1234567890}]  # Missing required keys

    with pytest.raises(KeyError):
        client.insert_rows(rows)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_missing_key_logs_details(
    mock_get_client: Mock, caplog
) -> None:
    """insert_rows() should log error details for missing key."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    rows = [{"timestamp": 1234567890}]  # Missing required keys

    with pytest.raises(KeyError):
        client.insert_rows(rows)

    assert "Failed to insert rows into ClickHouse" in caplog.text


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_insert_error(mock_get_client: Mock) -> None:
    """insert_rows() should raise exception on insert failure."""
    mock_client = Mock()
    mock_client.insert.side_effect = Exception("Insert failed")
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    rows = [
        {"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}
    ]

    with pytest.raises(Exception, match="Insert failed"):
        client.insert_rows(rows)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_rows_insert_error_logs_details(
    mock_get_client: Mock, caplog
) -> None:
    """insert_rows() should log error details on insert failure."""
    mock_client = Mock()
    mock_client.insert.side_effect = Exception("Insert failed")
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    rows = [
        {"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}
    ]

    with pytest.raises(Exception):
        client.insert_rows(rows)

    assert "Failed to insert rows into ClickHouse" in caplog.text
    assert "Insert failed" in caplog.text


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_success(
    mock_get_client: Mock, tmp_path
) -> None:
    """insert_from_file() should insert data from file successfully."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    # Create test file
    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
        '{"timestamp": 1234567900, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
    )

    client.insert_from_file(str(file_path))

    # insert_file is called with file object, not path string
    mock_client.insert_file.assert_called_once()
    call_args = mock_client.insert_file.call_args
    assert call_args[0][0] == "db.tbl"
    assert call_args[1]["column_names"] == [
        "timestamp",
        "metric_name",
        "labels",
        "value",
    ]
    assert call_args[1]["format_"] == "JSONEachRow"


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_not_found(
    mock_get_client: Mock, tmp_path
) -> None:
    """insert_from_file() should raise FileNotFoundError for missing file."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    with pytest.raises(FileNotFoundError):
        client.insert_from_file(str(tmp_path / "nonexistent.jsonl"))


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_empty_file(
    mock_get_client: Mock, tmp_path
) -> None:
    """insert_from_file() should handle empty file."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    # Create empty file
    file_path = tmp_path / "empty.jsonl"
    file_path.touch()

    client.insert_from_file(str(file_path))

    # Empty file still calls insert_file (but with empty data)
    mock_client.insert_file.assert_called_once()
    call_args = mock_client.insert_file.call_args
    assert call_args[0][0] == "db.tbl"
    assert call_args[1]["column_names"] == [
        "timestamp",
        "metric_name",
        "labels",
        "value",
    ]
    assert call_args[1]["format_"] == "JSONEachRow"


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_fallback_when_insert_file_unavailable(
    mock_get_client: Mock, tmp_path
) -> None:
    """insert_from_file() should fall back to insert_rows when insert_file unavailable."""  # noqa: E501
    mock_client = Mock()
    # Simulate insert_file not being available (AttributeError)
    del mock_client.insert_file
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    # Create test file
    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
    )

    client.insert_from_file(str(file_path))

    # Should fall back to insert_rows
    mock_client.insert.assert_called_once()
    call_args = mock_client.insert.call_args
    assert call_args[0][0] == "db.tbl"
    assert len(call_args[0][1]) == 1


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_insert_error(
    mock_get_client: Mock, tmp_path
) -> None:
    """insert_from_file() should raise exception on insert failure."""
    mock_client = Mock()
    mock_client.insert_file.side_effect = Exception("Insert failed")
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
    )

    with pytest.raises(Exception, match="Insert failed"):
        client.insert_from_file(str(file_path))


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_insert_error_logs_details(
    mock_get_client: Mock, tmp_path, caplog
) -> None:
    """insert_from_file() should log error details on insert failure."""
    mock_client = Mock()
    mock_client.insert_file.side_effect = Exception("Insert failed")
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
    )

    with pytest.raises(Exception):
        client.insert_from_file(str(file_path))

    assert "Failed to insert from file into ClickHouse" in caplog.text
    assert "Insert failed" in caplog.text


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_fallback_handles_empty_lines(
    mock_get_client: Mock, tmp_path
) -> None:
    """insert_from_file() fallback should skip empty lines."""
    mock_client = Mock()
    # Simulate insert_file not being available (AttributeError)
    del mock_client.insert_file
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    # Create test file with empty lines
    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
        "\n"  # Empty line
        '{"timestamp": 1234567900, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
        "\n"  # Empty line
    )

    # Should fall back to insert_rows and skip empty lines
    client.insert_from_file(str(file_path))

    # Verify insert_rows was called with only non-empty rows
    mock_client.insert.assert_called_once()
    call_args = mock_client.insert.call_args
    assert len(call_args[0][1]) == 2  # Both non-empty rows (empty lines are skipped)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_fallback_handles_empty_file(
    mock_get_client: Mock, tmp_path
) -> None:
    """insert_from_file() fallback should handle empty file gracefully."""
    mock_client = Mock()
    # Simulate insert_file not being available (AttributeError)
    del mock_client.insert_file
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    # Create empty file
    file_path = tmp_path / "empty.jsonl"
    file_path.touch()

    # Should not call insert_rows for empty file
    client.insert_from_file(str(file_path))

    # Verify insert_rows was not called (empty file, no rows)
    mock_client.insert.assert_not_called()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_get_state_success(mock_get_client: Mock) -> None:
    """get_state() should return state from ClickHouse."""
    mock_client = Mock()
    mock_result = Mock()
    mock_result.result_rows = [(1700000000, 1700000100, 1700000200, 300, 100)]
    mock_client.query.return_value = mock_result
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    state = client.get_state()

    assert state == {
        "timestamp_progress": 1700000000,
        "timestamp_start": 1700000100,
        "timestamp_end": 1700000200,
        "batch_window_seconds": 300,
        "batch_rows": 100,
    }
    mock_client.query.assert_called_once()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_get_state_empty_result(mock_get_client: Mock) -> None:
    """get_state() should return None values when no state exists."""
    mock_client = Mock()
    mock_result = Mock()
    mock_result.result_rows = []
    mock_client.query.return_value = mock_result
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    state = client.get_state()

    assert state == {
        "timestamp_progress": None,
        "timestamp_start": None,
        "timestamp_end": None,
        "batch_window_seconds": None,
        "batch_rows": None,
    }


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_get_state_with_nulls(mock_get_client: Mock) -> None:
    """get_state() should handle NULL values correctly."""
    mock_client = Mock()
    mock_result = Mock()
    mock_result.result_rows = [(1700000000, None, 1700000200, None, 100)]
    mock_client.query.return_value = mock_result
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    state = client.get_state()

    assert state == {
        "timestamp_progress": 1700000000,
        "timestamp_start": None,
        "timestamp_end": 1700000200,
        "batch_window_seconds": None,
        "batch_rows": 100,
    }


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_get_state_query_error(mock_get_client: Mock) -> None:
    """get_state() should raise exception on query failure."""
    mock_client = Mock()
    mock_client.query.side_effect = Exception("Query failed")
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    with pytest.raises(Exception, match="Query failed"):
        client.get_state()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_get_state_query_error_logs_details(
    mock_get_client: Mock, caplog
) -> None:
    """get_state() should log error details on query failure."""
    mock_client = Mock()
    mock_client.query.side_effect = Exception("Query failed")
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    with pytest.raises(Exception):
        client.get_state()

    assert "Failed to read state from ClickHouse" in caplog.text
    assert "Query failed" in caplog.text


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_get_state_invalid_table_name(mock_get_client: Mock) -> None:
    """get_state() should raise ValueError for invalid table name."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(state_table="invalid-table-name!")
    client = ClickHouseClient(cfg)

    with pytest.raises(ValueError, match="Invalid table name format"):
        client.get_state()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_success(mock_get_client: Mock) -> None:
    """save_state() should save state to ClickHouse."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    client.save_state(
        timestamp_progress=1700000000,
        timestamp_start=1700000100,
        timestamp_end=1700000200,
        batch_window_seconds=300,
        batch_rows=100,
    )

    mock_client.insert.assert_called_once_with(
        "metrics.etl_state",
        [[1700000000, 1700000100, 1700000200, 300, 100]],
        column_names=[
            "timestamp_progress",
            "timestamp_start",
            "timestamp_end",
            "batch_window_seconds",
            "batch_rows",
        ],
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_partial(mock_get_client: Mock) -> None:
    """save_state() should save only provided fields."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    client.save_state(
        timestamp_progress=1700000000,
        timestamp_start=1700000100,
    )

    mock_client.insert.assert_called_once_with(
        "metrics.etl_state",
        [[1700000000, 1700000100]],
        column_names=["timestamp_progress", "timestamp_start"],
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_empty(mock_get_client: Mock) -> None:
    """save_state() should not insert when no fields provided."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    client.save_state()

    mock_client.insert.assert_not_called()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_insert_error(mock_get_client: Mock) -> None:
    """save_state() should raise exception on insert failure."""
    mock_client = Mock()
    mock_client.insert.side_effect = Exception("Insert failed")
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    with pytest.raises(Exception, match="Insert failed"):
        client.save_state(timestamp_progress=1700000000)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_insert_error_logs_details(
    mock_get_client: Mock, caplog
) -> None:
    """save_state() should log error details on insert failure."""
    mock_client = Mock()
    mock_client.insert.side_effect = Exception("Insert failed")
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    with pytest.raises(Exception):
        client.save_state(timestamp_progress=1700000000)

    assert "Failed to save state to ClickHouse" in caplog.text
    assert "Insert failed" in caplog.text


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_custom_table(mock_get_client: Mock) -> None:
    """save_state() should use custom state table from config."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(state_table="custom.state_table")
    client = ClickHouseClient(cfg)

    client.save_state(timestamp_progress=1700000000)

    mock_client.insert.assert_called_once_with(
        "custom.state_table",
        [[1700000000]],
        column_names=["timestamp_progress"],
    )
