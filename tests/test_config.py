"""
Basic tests for configuration loading.
"""

import pytest

from config import load_config


def test_load_config_from_env(monkeypatch) -> None:
    """Config should load required values from environment."""
    monkeypatch.setenv("PROMETHEUS_URL", "http://prom:9090")
    monkeypatch.setenv("CLICKHOUSE_URL", "http://ch:8123")
    monkeypatch.setenv("CLICKHOUSE_TABLE", "db.tbl")

    config = load_config()

    assert config.prometheus.url == "http://prom:9090"
    assert config.clickhouse.table == "db.tbl"
    assert config.etl.batch_window_size_seconds > 0


def test_load_config_clickhouse_timeouts_from_env(monkeypatch) -> None:
    """Config should load ClickHouse timeout values from environment."""
    monkeypatch.setenv("PROMETHEUS_URL", "http://prom:9090")
    monkeypatch.setenv("CLICKHOUSE_URL", "http://ch:8123")
    monkeypatch.setenv("CLICKHOUSE_TABLE", "db.tbl")
    monkeypatch.setenv("CLICKHOUSE_CONNECT_TIMEOUT", "30")
    monkeypatch.setenv("CLICKHOUSE_SEND_RECEIVE_TIMEOUT", "600")

    config = load_config()

    assert config.clickhouse.connect_timeout == 30
    assert config.clickhouse.send_receive_timeout == 600


def test_load_config_validation_error_missing_required_field(monkeypatch) -> None:
    """load_config should raise ValueError on ValidationError for missing field."""
    # Remove required field
    monkeypatch.delenv("PROMETHEUS_URL", raising=False)
    monkeypatch.setenv("CLICKHOUSE_URL", "http://ch:8123")
    monkeypatch.setenv("CLICKHOUSE_TABLE", "db.tbl")

    with pytest.raises(ValueError, match="Configuration validation failed"):
        load_config()


def test_clickhouse_config_normalizes_password_when_user_specified(monkeypatch) -> None:
    """ClickHouseConfig should normalize None password to empty string when user is set.

    This handles the case when CLICKHOUSE_PASSWORD is set to empty string
    in environment variables and env_ignore_empty=True converts it to None.
    """
    from config import ClickHouseConfig

    # Simulate env_ignore_empty=True behavior: empty string becomes None
    cfg = ClickHouseConfig(
        _env_file=[],  # Disable .env file reading
        url="http://ch:8123",
        table="db.tbl",
        user="default",
        password=None,  # This should be normalized to ""
    )

    # Password should be normalized to empty string by validator
    assert cfg.password == ""


def test_clickhouse_config_keeps_none_password_when_no_user(monkeypatch) -> None:
    """ClickHouseConfig should keep None password when no user is specified."""
    from config import ClickHouseConfig

    cfg = ClickHouseConfig(
        _env_file=[],  # Disable .env file reading
        url="http://ch:8123",
        table="db.tbl",
        user=None,
        password=None,
    )

    # Password should remain None when no user is specified
    assert cfg.password is None


def test_prometheus_config_normalizes_password_when_user_specified(monkeypatch) -> None:
    """PrometheusConfig should normalize None password to empty string when user is set.

    This handles the case when PROMETHEUS_PASSWORD is set to empty string
    in environment variables and env_ignore_empty=True converts it to None.
    """
    from config import PrometheusConfig

    # Simulate env_ignore_empty=True behavior: empty string becomes None
    cfg = PrometheusConfig(
        _env_file=[],  # Disable .env file reading
        url="http://prom:9090",
        user="testuser",
        password=None,  # This should be normalized to ""
    )

    # Password should be normalized to empty string by validator
    assert cfg.password == ""


def test_prometheus_config_keeps_none_password_when_no_user(monkeypatch) -> None:
    """PrometheusConfig should keep None password when no user is specified."""
    from config import PrometheusConfig

    cfg = PrometheusConfig(
        _env_file=[],  # Disable .env file reading
        url="http://prom:9090",
        user=None,
        password=None,
    )

    # Password should remain None when no user is specified
    assert cfg.password is None


def test_clickhouse_config_has_default_table(monkeypatch) -> None:
    """ClickHouseConfig should have default table name when not specified."""
    from config import ClickHouseConfig

    cfg = ClickHouseConfig(
        _env_file=[],  # Disable .env file reading
        url="http://ch:8123",
    )

    # Table should have default value
    assert cfg.table == "default.metrics"


def test_clickhouse_config_has_default_table_state(monkeypatch) -> None:
    """ClickHouseConfig should have default table_state name when not specified."""
    from config import ClickHouseConfig

    cfg = ClickHouseConfig(
        _env_file=[],  # Disable .env file reading
        url="http://ch:8123",
        table="db.tbl",
    )

    # State table should have default value
    assert cfg.table_state == "default.etl"
