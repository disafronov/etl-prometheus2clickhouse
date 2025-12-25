"""
Basic tests for configuration loading.
"""

import pytest

from config import load_config


def test_load_config_from_env(monkeypatch) -> None:
    """Config should load required values from environment."""
    monkeypatch.setenv("PROMETHEUS_URL", "http://prom:9090")
    monkeypatch.setenv("CLICKHOUSE_URL", "http://ch:8123")
    monkeypatch.setenv("CLICKHOUSE_TABLE_METRICS", "db.tbl")

    config = load_config()

    assert config.prometheus.url == "http://prom:9090"
    assert config.clickhouse.table_metrics == "db.tbl"
    assert config.etl.batch_window_size_seconds > 0


def test_load_config_clickhouse_timeouts_from_env(monkeypatch) -> None:
    """Config should load ClickHouse timeout values from environment."""
    monkeypatch.setenv("PROMETHEUS_URL", "http://prom:9090")
    monkeypatch.setenv("CLICKHOUSE_URL", "http://ch:8123")
    monkeypatch.setenv("CLICKHOUSE_TABLE_METRICS", "db.tbl")
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
    monkeypatch.setenv("CLICKHOUSE_TABLE_METRICS", "db.tbl")

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
        table_metrics="db.tbl",
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
        table_metrics="db.tbl",
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


def test_clickhouse_config_has_default_table_metrics(monkeypatch) -> None:
    """ClickHouseConfig should have default table_metrics name when not specified."""
    from config import ClickHouseConfig

    cfg = ClickHouseConfig(
        _env_file=[],  # Disable .env file reading
        url="http://ch:8123",
    )

    # Table should have default value
    assert cfg.table_metrics == "default.metrics"


def test_clickhouse_config_has_default_table_etl(monkeypatch) -> None:
    """ClickHouseConfig should have default table_etl name when not specified."""
    from config import ClickHouseConfig

    cfg = ClickHouseConfig(
        _env_file=[],  # Disable .env file reading
        url="http://ch:8123",
        table_metrics="db.tbl",
    )

    # ETL table should have default value
    assert cfg.table_etl == "default.etl"


def test_etl_config_rejects_zero_batch_window_size(monkeypatch) -> None:
    """EtlConfig should reject batch_window_size_seconds equal to zero."""
    from pydantic import ValidationError

    from config import EtlConfig

    # Set environment variable to trigger validation
    monkeypatch.setenv("BATCH_WINDOW_SIZE_SECONDS", "0")
    monkeypatch.setenv("BATCH_WINDOW_OVERLAP_SECONDS", "0")

    with pytest.raises(ValidationError) as exc_info:
        EtlConfig()

    errors = exc_info.value.errors()
    assert len(errors) > 0
    # Check that error is for BATCH_WINDOW_SIZE_SECONDS (validation_alias)
    assert any(error["loc"] == ("BATCH_WINDOW_SIZE_SECONDS",) for error in errors)


def test_etl_config_rejects_negative_batch_window_size(monkeypatch) -> None:
    """EtlConfig should reject negative batch_window_size_seconds."""
    from pydantic import ValidationError

    from config import EtlConfig

    # Set environment variable to trigger validation
    monkeypatch.setenv("BATCH_WINDOW_SIZE_SECONDS", "-1")
    monkeypatch.setenv("BATCH_WINDOW_OVERLAP_SECONDS", "0")

    with pytest.raises(ValidationError) as exc_info:
        EtlConfig()

    errors = exc_info.value.errors()
    assert len(errors) > 0
    # Check that error is for BATCH_WINDOW_SIZE_SECONDS (validation_alias)
    assert any(error["loc"] == ("BATCH_WINDOW_SIZE_SECONDS",) for error in errors)


def test_etl_config_rejects_negative_batch_window_overlap(monkeypatch) -> None:
    """EtlConfig should reject negative batch_window_overlap_seconds."""
    from pydantic import ValidationError

    from config import EtlConfig

    # Set environment variable to trigger validation
    monkeypatch.setenv("BATCH_WINDOW_SIZE_SECONDS", "300")
    monkeypatch.setenv("BATCH_WINDOW_OVERLAP_SECONDS", "-1")

    with pytest.raises(ValidationError) as exc_info:
        EtlConfig()

    errors = exc_info.value.errors()
    assert len(errors) > 0
    # Check that error is for BATCH_WINDOW_OVERLAP_SECONDS (validation_alias)
    assert any(error["loc"] == ("BATCH_WINDOW_OVERLAP_SECONDS",) for error in errors)


def test_etl_config_accepts_valid_batch_window_values(monkeypatch) -> None:
    """EtlConfig should accept valid batch window configuration values."""
    from config import EtlConfig

    # Set environment variables to trigger validation
    monkeypatch.setenv("BATCH_WINDOW_SIZE_SECONDS", "300")
    monkeypatch.setenv("BATCH_WINDOW_OVERLAP_SECONDS", "20")

    cfg = EtlConfig()

    assert cfg.batch_window_size_seconds == 300
    assert cfg.batch_window_overlap_seconds == 20


def test_etl_config_accepts_zero_overlap(monkeypatch) -> None:
    """EtlConfig should accept zero overlap (default value)."""
    from config import EtlConfig

    # Set environment variable to trigger validation
    monkeypatch.setenv("BATCH_WINDOW_SIZE_SECONDS", "300")
    monkeypatch.setenv("BATCH_WINDOW_OVERLAP_SECONDS", "0")

    cfg = EtlConfig()

    assert cfg.batch_window_overlap_seconds == 0
