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
    monkeypatch.setenv("PUSHGATEWAY_URL", "http://pg:9091")
    monkeypatch.setenv("PUSHGATEWAY_JOB", "job")
    monkeypatch.setenv("PUSHGATEWAY_INSTANCE", "inst")

    config = load_config()

    assert config.prometheus.url == "http://prom:9090"
    assert config.clickhouse.table == "db.tbl"
    assert config.pushgateway.job == "job"
    assert config.etl.batch_window_seconds > 0


def test_load_config_clickhouse_timeouts_from_env(monkeypatch) -> None:
    """Config should load ClickHouse timeout values from environment."""
    monkeypatch.setenv("PROMETHEUS_URL", "http://prom:9090")
    monkeypatch.setenv("CLICKHOUSE_URL", "http://ch:8123")
    monkeypatch.setenv("CLICKHOUSE_TABLE", "db.tbl")
    monkeypatch.setenv("CLICKHOUSE_CONNECT_TIMEOUT", "30")
    monkeypatch.setenv("CLICKHOUSE_SEND_RECEIVE_TIMEOUT", "600")
    monkeypatch.setenv("PUSHGATEWAY_URL", "http://pg:9091")
    monkeypatch.setenv("PUSHGATEWAY_JOB", "job")
    monkeypatch.setenv("PUSHGATEWAY_INSTANCE", "inst")

    config = load_config()

    assert config.clickhouse.connect_timeout == 30
    assert config.clickhouse.send_receive_timeout == 600


def test_load_config_validation_error_missing_required_field(monkeypatch) -> None:
    """load_config should raise ValueError on ValidationError for missing field."""
    # Remove required field
    monkeypatch.delenv("PROMETHEUS_URL", raising=False)
    monkeypatch.setenv("CLICKHOUSE_URL", "http://ch:8123")
    monkeypatch.setenv("CLICKHOUSE_TABLE", "db.tbl")
    monkeypatch.setenv("PUSHGATEWAY_URL", "http://pg:9091")
    monkeypatch.setenv("PUSHGATEWAY_JOB", "job")
    monkeypatch.setenv("PUSHGATEWAY_INSTANCE", "inst")

    with pytest.raises(ValueError, match="Configuration validation failed"):
        load_config()
