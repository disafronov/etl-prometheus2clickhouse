"""
Comprehensive tests for EtlJob.
"""

from typing import Any

import pytest

from config import (
    ClickHouseConfig,
    Config,
    EtlConfig,
    PrometheusConfig,
    PushGatewayConfig,
)
from etl_job import EtlJob


class DummyPromClient:
    """Test double for PrometheusClient."""

    def __init__(self) -> None:
        self.query_calls: list[str] = []
        self.query_range_calls: list[dict[str, Any]] = []
        self._query_responses: dict[str, dict[str, Any]] = {}
        self._query_range_response: dict[str, Any] = {
            "status": "success",
            "data": {"result": []},
        }

    def set_query_response(self, expr: str, response: dict[str, Any]) -> None:
        """Set mock response for query(expr)."""
        self._query_responses[expr] = response

    def set_query_range_response(self, response: dict[str, Any]) -> None:
        """Set mock response for query_range()."""
        self._query_range_response = response

    def query(self, expr: str) -> dict[str, Any]:
        """Mock query method."""
        self.query_calls.append(expr)
        if expr in self._query_responses:
            return self._query_responses[expr]
        return {"status": "success", "data": {"result": []}}

    def query_range(
        self,
        expr: str,
        start: float,
        end: float,
        step: str,
    ) -> dict[str, Any]:
        """Mock query_range method."""
        self.query_range_calls.append(
            {"expr": expr, "start": start, "end": end, "step": step}
        )
        return self._query_range_response


class DummyClickHouseClient:
    """Test double for ClickHouseClient."""

    def __init__(self) -> None:
        self.inserts: list[list[dict[str, Any]]] = []
        self._should_fail = False

    def set_should_fail(self, should_fail: bool) -> None:
        """Configure whether insert should fail."""
        self._should_fail = should_fail

    def insert_rows(self, rows: list[dict[str, Any]]) -> None:
        """Mock insert_rows method."""
        if self._should_fail:
            raise Exception("ClickHouse insert failed")
        self.inserts.append(rows)


class DummyPushGatewayClient:
    """Test double for PushGatewayClient."""

    def __init__(self) -> None:
        self.starts: list[float] = []
        self.success_calls: list[dict[str, Any]] = []
        self._should_fail_start = False
        self._should_fail_success = False

    def set_should_fail_start(self, should_fail: bool) -> None:
        """Configure whether push_start should fail."""
        self._should_fail_start = should_fail

    def set_should_fail_success(self, should_fail: bool) -> None:
        """Configure whether push_success should fail."""
        self._should_fail_success = should_fail

    def push_start(self, timestamp_start: float) -> None:
        """Mock push_start method."""
        if self._should_fail_start:
            raise Exception("PushGateway push_start failed")
        self.starts.append(timestamp_start)

    def push_success(
        self,
        timestamp_end: float,
        timestamp_progress: float,
        window_seconds: int,
        rows_count: int,
    ) -> None:
        """Mock push_success method."""
        if self._should_fail_success:
            raise Exception("PushGateway push_success failed")
        self.success_calls.append(
            {
                "timestamp_end": timestamp_end,
                "timestamp_progress": timestamp_progress,
                "window_seconds": window_seconds,
                "rows_count": rows_count,
            }
        )


def _make_config() -> Config:
    """Create minimal valid Config for tests."""
    return Config(
        prometheus=PrometheusConfig(
            url="http://prom:9090",
        ),
        clickhouse=ClickHouseConfig(
            url="http://ch:8123",
            table="db.tbl",
        ),
        pushgateway=PushGatewayConfig(
            url="http://pg:9091",
            job="job",
            instance="inst",
        ),
        etl=EtlConfig(batch_window_seconds=300),
    )


def test_etl_job_run_once_success() -> None:
    """EtlJob.run_once should complete successfully in happy path."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    # Mock Prometheus responses
    prom.set_query_response(
        "etl_timestamp_start",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )
    prom.set_query_response(
        "etl_timestamp_end",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000100"]}]},
        },
    )
    prom.set_query_response(
        "etl_timestamp_progress",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )

    # Mock query_range response with sample data
    prom.set_query_range_response(
        {
            "status": "success",
            "data": {
                "result": [
                    {
                        "metric": {"__name__": "up", "instance": "localhost"},
                        "values": [[1700000000, "1"], [1700000300, "1"]],
                    }
                ]
            },
        }
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    job.run_once()

    # Verify calls
    assert len(pg.starts) == 1
    assert len(ch.inserts) == 1
    assert len(ch.inserts[0]) == 2  # Two data points
    assert len(pg.success_calls) == 1
    assert pg.success_calls[0]["rows_count"] == 2


def test_etl_job_run_once_cannot_start_when_end_less_than_start() -> None:
    """EtlJob should not start when TimestampEnd < TimestampStart."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    # Set end < start (previous job still running)
    prom.set_query_response(
        "etl_timestamp_start",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000100"]}]},
        },
    )
    prom.set_query_response(
        "etl_timestamp_end",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    job.run_once()

    # Should not proceed
    assert len(pg.starts) == 0
    assert len(ch.inserts) == 0
    assert len(pg.success_calls) == 0


def test_etl_job_run_once_can_start_when_end_exists_but_start_missing() -> None:
    """EtlJob should start when TimestampEnd exists but TimestampStart missing."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    # End exists but start doesn't - inconsistent state, but previous job finished
    prom.set_query_response(
        "etl_timestamp_start", {"status": "success", "data": {"result": []}}
    )
    prom.set_query_response(
        "etl_timestamp_end",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )
    # Progress metric is required
    prom.set_query_response(
        "etl_timestamp_progress",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )

    prom.set_query_range_response(
        {
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
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    job.run_once()

    # Should proceed despite inconsistent state (previous job finished)
    assert len(pg.starts) == 1
    assert len(ch.inserts) == 1


def test_etl_job_run_once_cannot_start_when_start_exists_but_end_missing() -> None:
    """EtlJob should not start when TimestampStart exists but TimestampEnd missing."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    # Start exists but end doesn't - previous job is still running
    prom.set_query_response(
        "etl_timestamp_start",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )
    prom.set_query_response(
        "etl_timestamp_end", {"status": "success", "data": {"result": []}}
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    job.run_once()

    # Should not proceed
    assert len(pg.starts) == 0
    assert len(ch.inserts) == 0
    assert len(pg.success_calls) == 0


def test_etl_job_run_once_can_start_when_no_previous_run() -> None:
    """EtlJob should start when no previous run info exists."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    # No previous run (empty results)
    prom.set_query_response(
        "etl_timestamp_start", {"status": "success", "data": {"result": []}}
    )
    prom.set_query_response(
        "etl_timestamp_end", {"status": "success", "data": {"result": []}}
    )
    # Progress metric is required
    prom.set_query_response(
        "etl_timestamp_progress",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )

    prom.set_query_range_response(
        {
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
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    job.run_once()

    # Should proceed
    assert len(pg.starts) == 1
    assert len(ch.inserts) == 1


def test_etl_job_run_once_fails_on_mark_start_error() -> None:
    """EtlJob should stop if push_start fails."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    pg.set_should_fail_start(True)

    prom.set_query_response(
        "etl_timestamp_start", {"status": "success", "data": {"result": []}}
    )
    prom.set_query_response(
        "etl_timestamp_end", {"status": "success", "data": {"result": []}}
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    job.run_once()

    # Should not proceed after failed start
    assert len(ch.inserts) == 0
    assert len(pg.success_calls) == 0


def test_etl_job_run_once_fails_on_fetch_error() -> None:
    """EtlJob should raise exception if fetch_data fails."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    prom.set_query_response(
        "etl_timestamp_start", {"status": "success", "data": {"result": []}}
    )
    prom.set_query_response(
        "etl_timestamp_end", {"status": "success", "data": {"result": []}}
    )
    # Progress metric is required
    prom.set_query_response(
        "etl_timestamp_progress",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )

    # Make query_range raise exception
    def failing_query_range(*_: object, **__: object) -> dict[str, Any]:
        raise Exception("Prometheus query_range failed")

    prom.query_range = failing_query_range  # type: ignore[assignment]

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    with pytest.raises(Exception, match="Prometheus query_range failed"):
        job.run_once()

    # Should not write or push success
    assert len(ch.inserts) == 0
    assert len(pg.success_calls) == 0


def test_etl_job_run_once_fails_on_write_error() -> None:
    """EtlJob should raise exception if write_rows fails."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    prom.set_query_response(
        "etl_timestamp_start", {"status": "success", "data": {"result": []}}
    )
    prom.set_query_response(
        "etl_timestamp_end", {"status": "success", "data": {"result": []}}
    )
    # Progress metric is required
    prom.set_query_response(
        "etl_timestamp_progress",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )
    prom.set_query_range_response(
        {
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
    )

    ch.set_should_fail(True)

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    with pytest.raises(Exception, match="ClickHouse insert failed"):
        job.run_once()

    # Should not push success metrics
    assert len(pg.success_calls) == 0


def test_etl_job_run_once_fails_when_progress_missing() -> None:
    """EtlJob should fail when TimestampProgress is not found in Prometheus."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    prom.set_query_response(
        "etl_timestamp_start", {"status": "success", "data": {"result": []}}
    )
    prom.set_query_response(
        "etl_timestamp_end", {"status": "success", "data": {"result": []}}
    )
    # No progress metric - should cause failure
    prom.set_query_response(
        "etl_timestamp_progress", {"status": "success", "data": {"result": []}}
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    with pytest.raises(ValueError, match="TimestampProgress.*not found"):
        job.run_once()

    # Should not proceed beyond progress loading
    # Note: _mark_start is called before _load_progress, so timestamp_start is pushed
    assert len(pg.starts) == 1  # Start was marked before progress check
    assert len(ch.inserts) == 0  # But no data was processed
    assert len(pg.success_calls) == 0  # And no success metrics


def test_etl_job_run_once_fails_on_push_success_error() -> None:
    """EtlJob should raise exception if push_success fails after successful write."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    prom.set_query_response(
        "etl_timestamp_start", {"status": "success", "data": {"result": []}}
    )
    prom.set_query_response(
        "etl_timestamp_end", {"status": "success", "data": {"result": []}}
    )
    # Progress metric is required
    prom.set_query_response(
        "etl_timestamp_progress",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )
    prom.set_query_range_response(
        {
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
    )

    # Make push_success fail
    pg.set_should_fail_success(True)

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    with pytest.raises(Exception, match="PushGateway push_success failed"):
        job.run_once()

    # Data should be written to ClickHouse before push_success fails
    assert len(ch.inserts) == 1
    assert len(ch.inserts[0]) == 1
    # But push_success should not succeed
    assert len(pg.success_calls) == 0


def test_etl_job_fetch_data_parses_prometheus_response() -> None:
    """EtlJob._fetch_data should correctly parse Prometheus query_range response."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    prom.set_query_response(
        "etl_timestamp_start", {"status": "success", "data": {"result": []}}
    )
    prom.set_query_response(
        "etl_timestamp_end", {"status": "success", "data": {"result": []}}
    )
    # Progress metric is required
    prom.set_query_response(
        "etl_timestamp_progress",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )

    # Complex response with multiple series and labels
    prom.set_query_range_response(
        {
            "status": "success",
            "data": {
                "result": [
                    {
                        "metric": {
                            "__name__": "http_requests_total",
                            "method": "GET",
                            "status": "200",
                        },
                        "values": [
                            [1700000000, "10"],
                            [1700000300, "15"],
                        ],
                    },
                    {
                        "metric": {
                            "__name__": "http_requests_total",
                            "method": "POST",
                            "status": "200",
                        },
                        "values": [[1700000000, "5"]],
                    },
                ]
            },
        }
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    job.run_once()

    # Should parse all data points
    assert len(ch.inserts) == 1
    rows = ch.inserts[0]
    assert len(rows) == 3  # 2 + 1 data points

    # Check first row structure
    row = rows[0]
    assert row["metric_name"] == "http_requests_total"
    # ClickHouse DateTime requires integer Unix timestamp
    assert row["timestamp"] == 1700000000
    assert row["value"] == 10.0
    # Labels are serialized to JSON string
    import json

    labels = json.loads(row["labels"])
    assert "method" in labels
    assert labels["method"] == "GET"
    assert "status" in labels
    assert labels["status"] == "200"
    assert "__name__" not in labels  # Should be removed


def test_etl_job_check_can_start_handles_query_exception() -> None:
    """EtlJob._check_can_start should handle exceptions from Prometheus query."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    # Make query raise exception
    def failing_query(*_: object, **__: object) -> dict[str, Any]:
        raise Exception("Prometheus query failed")

    prom.query = failing_query  # type: ignore[assignment]

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    # Should return False and not raise exception
    result = job._check_can_start()
    assert result is False
    assert len(pg.starts) == 0


def test_etl_job_load_progress_handles_query_exception() -> None:
    """EtlJob._load_progress should raise exception when query fails."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    prom.set_query_response(
        "etl_timestamp_start", {"status": "success", "data": {"result": []}}
    )
    prom.set_query_response(
        "etl_timestamp_end", {"status": "success", "data": {"result": []}}
    )

    # Make query raise exception for progress metric
    def failing_query(expr: str) -> dict[str, Any]:
        if expr == "etl_timestamp_progress":
            raise Exception("Prometheus query failed")
        return {"status": "success", "data": {"result": []}}

    prom.query = failing_query  # type: ignore[assignment]

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    # Mark start first (required for load_progress to be called)
    job._mark_start(1700000000.0)

    # Should raise exception
    with pytest.raises(Exception, match="Prometheus query failed"):
        job._load_progress()


def test_etl_job_read_gauge_handles_non_success_status() -> None:
    """EtlJob._read_gauge should return None when Prometheus returns non-success."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    # Set response with error status
    prom.set_query_response(
        "etl_timestamp_start", {"status": "error", "data": {"result": []}}
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    result = job._read_gauge("etl_timestamp_start")
    assert result is None


def test_etl_job_read_gauge_handles_invalid_value_format() -> None:
    """EtlJob._read_gauge should return None when value format is invalid."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    # Set response with invalid value format (missing value field)
    prom.set_query_response(
        "etl_timestamp_start",
        {
            "status": "success",
            "data": {
                "result": [{"value": []}]
            },  # Invalid: value should have 2 elements
        },
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    result = job._read_gauge("etl_timestamp_start")
    assert result is None


def test_etl_job_fetch_data_handles_invalid_value_pairs() -> None:
    """EtlJob._fetch_data should skip invalid value pairs in Prometheus response."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()
    pg = DummyPushGatewayClient()

    prom.set_query_response(
        "etl_timestamp_start", {"status": "success", "data": {"result": []}}
    )
    prom.set_query_response(
        "etl_timestamp_end", {"status": "success", "data": {"result": []}}
    )
    prom.set_query_response(
        "etl_timestamp_progress",
        {
            "status": "success",
            "data": {"result": [{"value": [1234567890, "1700000000"]}]},
        },
    )

    # Response with invalid value pairs (should be skipped)
    prom.set_query_range_response(
        {
            "status": "success",
            "data": {
                "result": [
                    {
                        "metric": {"__name__": "up"},
                        "values": [
                            [1700000000, "1"],  # Valid
                            [1700000300],  # Invalid: missing value
                            ["invalid", "2"],  # Invalid: timestamp not integer
                            [1700000600, "3"],  # Valid
                        ],
                    }
                ]
            },
        }
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
        pushgateway_client=pg,
    )

    job.run_once()

    # Should only process valid value pairs (2 out of 4)
    assert len(ch.inserts) == 1
    assert len(ch.inserts[0]) == 2
