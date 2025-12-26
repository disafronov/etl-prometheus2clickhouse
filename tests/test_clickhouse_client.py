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
        "table_metrics": "db.tbl",
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
    assert client._table_metrics == "db.tbl"
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
        verify=True,
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
        verify=True,
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
    # Password is normalized to empty string when user is specified
    mock_get_client.assert_called_once_with(
        host="ch",
        port=8123,
        username="user",
        password="",
        secure=False,
        verify=True,
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
        verify=True,
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
@patch("clickhouse_client.logger")
def test_clickhouse_client_init_connection_error_logs_details(
    mock_logger: Mock, mock_get_client: Mock
) -> None:
    """Client should log connection error details."""
    mock_get_client.side_effect = Exception("Connection failed")

    cfg = _make_clickhouse_config()
    with pytest.raises(Exception):
        ClickHouseClient(cfg)

    # Check that error was logged
    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args
    assert "Failed to create ClickHouse client" in call_args[0][0]
    assert "Connection failed" in call_args[0][0]


@patch("clickhouse_client.requests.post")
@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_success(
    mock_get_client: Mock, mock_requests_post: Mock, tmp_path
) -> None:
    """insert_from_file() should insert data via HTTP streaming."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_response = Mock()
    mock_response.raise_for_status = Mock()
    mock_requests_post.return_value = mock_response

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    # Create test file
    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
        '{"timestamp": 1234567900, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
    )

    client.insert_from_file(str(file_path))

    # Verify HTTP POST was called with correct parameters
    mock_requests_post.assert_called_once()
    call_args = mock_requests_post.call_args
    assert call_args[0][0] == "http://ch:8123"
    assert call_args[1]["params"]["query"] == "INSERT INTO db.tbl FORMAT JSONEachRow"
    assert "data" in call_args[1]
    mock_response.raise_for_status.assert_called_once()


@patch("clickhouse_client.requests.post")
@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_invalid_table_name(
    mock_get_client: Mock, mock_requests_post: Mock, tmp_path
) -> None:
    """insert_from_file() should raise ValueError for invalid table name."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_metrics="invalid-table-name!")
    client = ClickHouseClient(cfg)

    # Create test file
    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
    )

    with pytest.raises(ValueError, match="Invalid table_metrics format"):
        client.insert_from_file(str(file_path))

    # Verify that HTTP POST was not called due to validation error
    mock_requests_post.assert_not_called()


@patch("clickhouse_client.requests.post")
@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_empty_table_name(
    mock_get_client: Mock, mock_requests_post: Mock, tmp_path
) -> None:
    """insert_from_file() should raise ValueError for empty table name."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_metrics="")
    client = ClickHouseClient(cfg)

    # Create test file
    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
    )

    with pytest.raises(ValueError, match="table name cannot be empty"):
        client.insert_from_file(str(file_path))

    # Verify that HTTP POST was not called due to validation error
    mock_requests_post.assert_not_called()


@patch("clickhouse_client.requests.post")
@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_too_many_dots(
    mock_get_client: Mock, mock_requests_post: Mock, tmp_path
) -> None:
    """insert_from_file() should raise ValueError for table name with too many dots."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_metrics="db.table.extra")
    client = ClickHouseClient(cfg)

    # Create test file
    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
    )

    with pytest.raises(ValueError, match="too many dots"):
        client.insert_from_file(str(file_path))

    # Verify that HTTP POST was not called due to validation error
    mock_requests_post.assert_not_called()


@patch("clickhouse_client.requests.post")
@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_empty_part(
    mock_get_client: Mock, mock_requests_post: Mock, tmp_path
) -> None:
    """insert_from_file() should raise ValueError for table name with empty part."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_metrics=".table")
    client = ClickHouseClient(cfg)

    # Create test file
    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
    )

    with pytest.raises(ValueError, match="empty part"):
        client.insert_from_file(str(file_path))

    # Verify that HTTP POST was not called due to validation error
    mock_requests_post.assert_not_called()


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


@patch("clickhouse_client.requests.post")
@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_empty_file(
    mock_get_client: Mock, mock_requests_post: Mock, tmp_path
) -> None:
    """insert_from_file() should handle empty file without HTTP POST."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    # Create empty file
    file_path = tmp_path / "empty.jsonl"
    file_path.touch()

    client.insert_from_file(str(file_path))

    # Empty file should not call HTTP POST to avoid unnecessary request
    mock_requests_post.assert_not_called()


@patch("clickhouse_client.requests.post")
@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_insert_from_file_insert_error(
    mock_get_client: Mock, mock_requests_post: Mock, tmp_path
) -> None:
    """insert_from_file() should raise exception on HTTP POST failure."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_requests_post.side_effect = Exception("HTTP request failed")

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
    )

    with pytest.raises(Exception, match="HTTP request failed"):
        client.insert_from_file(str(file_path))


@patch("clickhouse_client.requests.post")
@patch("clickhouse_client.clickhouse_connect.get_client")
@patch("clickhouse_client.logger")
def test_clickhouse_client_insert_from_file_insert_error_logs_details(
    mock_logger: Mock, mock_get_client: Mock, mock_requests_post: Mock, tmp_path
) -> None:
    """insert_from_file() should log error details on HTTP POST failure."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_requests_post.side_effect = Exception("HTTP request failed")

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    file_path = tmp_path / "test.jsonl"
    file_path.write_text(
        '{"timestamp": 1234567890, "metric_name": "up", "labels": "{}", "value": 1.0}\n'
    )

    with pytest.raises(Exception):
        client.insert_from_file(str(file_path))

    # Check that error was logged
    assert mock_logger.error.call_count >= 1
    error_messages = [call[0][0] for call in mock_logger.error.call_args_list]
    assert any(
        "Failed to insert from file into ClickHouse via HTTP streaming" in msg
        for msg in error_messages
    )
    assert any("HTTP request failed" in msg for msg in error_messages)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_get_state_success(mock_get_client: Mock) -> None:
    """get_state() should return state from ClickHouse."""
    mock_client = Mock()
    mock_result = Mock()
    # Order: timestamp_start, timestamp_end, timestamp_progress,
    # batch_window_seconds, batch_rows
    mock_result.result_rows = [(1700000100, 1700000200, 1700000000, 300, 100)]
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
    # Order: timestamp_start, timestamp_end, timestamp_progress,
    # batch_window_seconds, batch_rows
    mock_result.result_rows = [(1700000100, None, 1700000000, None, 100)]
    mock_client.query.return_value = mock_result
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    state = client.get_state()

    assert state == {
        "timestamp_progress": 1700000000,
        "timestamp_start": 1700000100,
        "timestamp_end": None,
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
@patch("clickhouse_client.logger")
def test_clickhouse_client_get_state_query_error_logs_details(
    mock_logger: Mock, mock_get_client: Mock
) -> None:
    """get_state() should log error details on query failure."""
    mock_client = Mock()
    mock_client.query.side_effect = Exception("Query failed")
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    with pytest.raises(Exception):
        client.get_state()

    # Check that error was logged (may be called multiple times due to
    # validation errors)
    assert mock_logger.error.call_count >= 1
    error_messages = [call[0][0] for call in mock_logger.error.call_args_list]
    assert any("Failed to read state from ClickHouse" in msg for msg in error_messages)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_get_state_invalid_table_name(mock_get_client: Mock) -> None:
    """get_state() should raise ValueError for invalid table name."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_etl="invalid-table-name!")
    client = ClickHouseClient(cfg)

    with pytest.raises(ValueError, match="Invalid table_etl format"):
        client.get_state()

    # Verify that query was not called due to validation error
    mock_client.query.assert_not_called()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_get_state_empty_table_name(mock_get_client: Mock) -> None:
    """get_state() should raise ValueError for empty table name."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_etl="")
    client = ClickHouseClient(cfg)

    with pytest.raises(ValueError, match="table name cannot be empty"):
        client.get_state()

    # Verify that query was not called due to validation error
    mock_client.query.assert_not_called()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_get_state_too_many_dots(mock_get_client: Mock) -> None:
    """get_state() should raise ValueError for table name with too many dots."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_etl="db.table.extra")
    client = ClickHouseClient(cfg)

    with pytest.raises(ValueError, match="too many dots"):
        client.get_state()

    # Verify that query was not called due to validation error
    mock_client.query.assert_not_called()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_get_state_empty_part(mock_get_client: Mock) -> None:
    """get_state() should raise ValueError for table name with empty part."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_etl=".table")
    client = ClickHouseClient(cfg)

    with pytest.raises(ValueError, match="empty part"):
        client.get_state()

    # Verify that query was not called due to validation error
    mock_client.query.assert_not_called()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_success(mock_get_client: Mock) -> None:
    """save_state() should insert state using INSERT."""
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

    # Always uses INSERT (ReplacingMergeTree handles deduplication)
    # Fields saved in table order:
    # timestamp_start, timestamp_end, timestamp_progress, ...
    mock_client.insert.assert_called_once_with(
        "default.etl",
        [[1700000100, 1700000200, 1700000000, 300, 100]],
        column_names=[
            "timestamp_start",
            "timestamp_end",
            "timestamp_progress",
            "batch_window_seconds",
            "batch_rows",
        ],
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_partial(mock_get_client: Mock) -> None:
    """save_state() should insert only provided fields."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    client.save_state(
        timestamp_progress=1700000000,
        timestamp_start=1700000100,
    )

    # Always uses INSERT with only provided fields
    # Fields saved in table order:
    # timestamp_start, timestamp_end, timestamp_progress, ...
    mock_client.insert.assert_called_once_with(
        "default.etl",
        [[1700000100, 1700000000]],
        column_names=["timestamp_start", "timestamp_progress"],
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
def test_clickhouse_client_save_state_insert_new_record(mock_get_client: Mock) -> None:
    """save_state() should insert new record when timestamp_start is not provided."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    client.save_state(
        timestamp_progress=1700000000,
        timestamp_end=1700000200,
    )

    # When timestamp_start is not provided, should use INSERT
    # Fields saved in table order:
    # timestamp_start, timestamp_end, timestamp_progress, ...
    mock_client.insert.assert_called_once_with(
        "default.etl",
        [[1700000200, 1700000000]],
        column_names=["timestamp_end", "timestamp_progress"],
    )


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
@patch("clickhouse_client.logger")
def test_clickhouse_client_save_state_insert_error_logs_details(
    mock_logger: Mock, mock_get_client: Mock
) -> None:
    """save_state() should log error details on insert failure."""
    mock_client = Mock()
    mock_client.insert.side_effect = Exception("Insert failed")
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    with pytest.raises(Exception):
        client.save_state(timestamp_progress=1700000000)

    # Check that error was logged (may be called multiple times due to
    # validation errors)
    assert mock_logger.error.call_count >= 1
    error_messages = [call[0][0] for call in mock_logger.error.call_args_list]
    assert any("Failed to save state to ClickHouse" in msg for msg in error_messages)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_custom_table(mock_get_client: Mock) -> None:
    """save_state() should use custom state table from config."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_etl="custom.state_table")
    client = ClickHouseClient(cfg)

    client.save_state(timestamp_progress=1700000000)

    mock_client.insert.assert_called_once_with(
        "custom.state_table",
        [[1700000000]],
        column_names=["timestamp_progress"],
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_update_no_other_fields(
    mock_get_client: Mock,
) -> None:
    """save_state() should insert when timestamp_start provided but no other fields."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    client.save_state(timestamp_start=1700000100)

    # Always uses INSERT
    mock_client.insert.assert_called_once_with(
        "default.etl",
        [[1700000100]],
        column_names=["timestamp_start"],
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_update_with_end_only(
    mock_get_client: Mock,
) -> None:
    """save_state() should insert when only timestamp_end provided."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    client.save_state(
        timestamp_start=1700000100,
        timestamp_end=1700000200,
    )

    # Always uses INSERT with only provided fields
    mock_client.insert.assert_called_once_with(
        "default.etl",
        [[1700000100, 1700000200]],
        column_names=["timestamp_start", "timestamp_end"],
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_update_invalid_table_name(
    mock_get_client: Mock,
) -> None:
    """save_state() should raise ValueError for invalid table name."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_etl="invalid-table-name!")
    client = ClickHouseClient(cfg)

    with pytest.raises(ValueError, match="Invalid table_etl format"):
        client.save_state(
            timestamp_start=1700000100,
            timestamp_progress=1700000000,
        )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_empty_table_name(
    mock_get_client: Mock,
) -> None:
    """save_state() should raise ValueError for empty table name."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_etl="")
    client = ClickHouseClient(cfg)

    with pytest.raises(ValueError, match="table name cannot be empty"):
        client.save_state(timestamp_start=1700000100)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_too_many_dots(
    mock_get_client: Mock,
) -> None:
    """save_state() should raise ValueError for table name with too many dots."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_etl="db.table.extra")
    client = ClickHouseClient(cfg)

    with pytest.raises(ValueError, match="too many dots"):
        client.save_state(timestamp_start=1700000100)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_empty_part(
    mock_get_client: Mock,
) -> None:
    """save_state() should raise ValueError for table name with empty part."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config(table_etl=".table")
    client = ClickHouseClient(cfg)

    with pytest.raises(ValueError, match="empty part"):
        client.save_state(timestamp_start=1700000100)


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_insert_with_start_only(
    mock_get_client: Mock,
) -> None:
    """save_state() should insert when only timestamp_start provided."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    client.save_state(timestamp_start=1700000100)

    mock_client.insert.assert_called_once_with(
        "default.etl",
        [[1700000100]],
        column_names=["timestamp_start"],
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_save_state_insert_with_batch_fields(
    mock_get_client: Mock,
) -> None:
    """save_state() should insert with batch_window_seconds and batch_rows."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    client.save_state(
        batch_window_seconds=300,
        batch_rows=100,
    )

    mock_client.insert.assert_called_once_with(
        "default.etl",
        [[300, 100]],
        column_names=["batch_window_seconds", "batch_rows"],
    )


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_has_running_job_when_running_exists(
    mock_get_client: Mock,
) -> None:
    """has_running_job() should return True when running job exists."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    # Mock query result with running job (timestamp_start but no timestamp_end)
    mock_result = Mock()
    mock_result.result_rows = [[1700000100]]  # timestamp_start exists
    mock_client.query.return_value = mock_result

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    result = client.has_running_job()

    assert result is True
    mock_client.query.assert_called_once()
    # Verify query checks for running job
    query_call = mock_client.query.call_args[0][0]
    assert "timestamp_start IS NOT NULL" in query_call
    assert "timestamp_end IS NULL" in query_call


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_has_running_job_when_no_running(
    mock_get_client: Mock,
) -> None:
    """has_running_job() should return False when no running job exists."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    # Mock query result with no running job
    mock_result = Mock()
    mock_result.result_rows = []  # No running job
    mock_client.query.return_value = mock_result

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    result = client.has_running_job()

    assert result is False
    mock_client.query.assert_called_once()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_has_running_job_handles_query_error(
    mock_get_client: Mock,
) -> None:
    """has_running_job() should raise exception when query fails."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_client.query.side_effect = Exception("Query failed")

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    with pytest.raises(Exception, match="Query failed"):
        client.has_running_job()


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_try_mark_start_success(mock_get_client: Mock) -> None:
    """try_mark_start() should return True when start is marked successfully."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    # First query (INSERT) succeeds
    # Second query (verification) returns our timestamp_start as only running job
    mock_result_verify = Mock()
    mock_result_verify.result_rows = [[1700000100]]  # Our timestamp_start
    mock_client.query.side_effect = [None, mock_result_verify]  # INSERT, then verify

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    result = client.try_mark_start(1700000100)

    assert result is True
    assert mock_client.query.call_count == 2  # INSERT + verification


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_try_mark_start_when_other_job_running(
    mock_get_client: Mock,
) -> None:
    """try_mark_start() should return False when another job is running."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    # INSERT succeeds, but verification shows different timestamp_start
    mock_result_verify = Mock()
    mock_result_verify.result_rows = [[1700000200]]  # Different timestamp_start
    mock_client.query.side_effect = [None, mock_result_verify]

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    result = client.try_mark_start(1700000100)

    assert result is False
    assert mock_client.query.call_count == 2


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_try_mark_start_when_multiple_jobs_running(
    mock_get_client: Mock,
) -> None:
    """try_mark_start() should return False when multiple jobs are running."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    # INSERT succeeds, but verification shows multiple running jobs
    mock_result_verify = Mock()
    mock_result_verify.result_rows = [[1700000200], [1700000100]]  # Multiple jobs
    mock_client.query.side_effect = [None, mock_result_verify]

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    result = client.try_mark_start(1700000100)

    assert result is False
    assert mock_client.query.call_count == 2


@patch("clickhouse_client.clickhouse_connect.get_client")
def test_clickhouse_client_try_mark_start_handles_query_error(
    mock_get_client: Mock,
) -> None:
    """try_mark_start() should raise exception when query fails."""
    mock_client = Mock()
    mock_get_client.return_value = mock_client

    mock_client.query.side_effect = Exception("Query failed")

    cfg = _make_clickhouse_config()
    client = ClickHouseClient(cfg)

    with pytest.raises(Exception, match="Query failed"):
        client.try_mark_start(1700000100)
