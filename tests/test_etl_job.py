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
        start: int,
        end: int,
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
        self.insert_from_file_calls: list[str] = []
        self._should_fail = False
        self._state: dict[str, int | None] = {
            "timestamp_progress": None,
            "timestamp_start": None,
            "timestamp_end": None,
            "batch_window_seconds": None,
            "batch_rows": None,
        }
        self._should_fail_get_state = False
        self._should_fail_save_state = False

    def set_should_fail(self, should_fail: bool) -> None:
        """Configure whether insert should fail."""
        self._should_fail = should_fail

    def set_should_fail_get_state(self, should_fail: bool) -> None:
        """Configure whether get_state should fail."""
        self._should_fail_get_state = should_fail

    def set_should_fail_save_state(self, should_fail: bool) -> None:
        """Configure whether save_state should fail."""
        self._should_fail_save_state = should_fail

    def set_state(
        self,
        timestamp_progress: int | None = None,
        timestamp_start: int | None = None,
        timestamp_end: int | None = None,
        batch_window_seconds: int | None = None,
        batch_rows: int | None = None,
    ) -> None:
        """Set state directly for testing purposes."""
        if timestamp_progress is not None:
            self._state["timestamp_progress"] = timestamp_progress
        if timestamp_start is not None:
            self._state["timestamp_start"] = timestamp_start
        if timestamp_end is not None:
            self._state["timestamp_end"] = timestamp_end
        if batch_window_seconds is not None:
            self._state["batch_window_seconds"] = batch_window_seconds
        if batch_rows is not None:
            self._state["batch_rows"] = batch_rows

    def insert_rows(self, rows: list[dict[str, Any]]) -> None:
        """Mock insert_rows method."""
        if self._should_fail:
            raise Exception("ClickHouse insert failed")
        self.inserts.append(rows)

    def insert_from_file(self, file_path: str) -> None:
        """Mock insert_from_file method."""
        if self._should_fail:
            raise Exception("ClickHouse insert failed")
        self.insert_from_file_calls.append(file_path)
        # For testing purposes, read file and store as rows
        import json
        import os

        if os.path.getsize(file_path) == 0:
            return

        rows: list[dict[str, Any]] = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                rows.append(row)
        if rows:
            self.inserts.append(rows)

    def get_state(self) -> dict[str, int | None]:
        """Mock get_state method."""
        if self._should_fail_get_state:
            raise Exception("ClickHouse get_state failed")
        return self._state.copy()

    def save_state(
        self,
        timestamp_progress: int | None = None,
        timestamp_start: int | None = None,
        timestamp_end: int | None = None,
        batch_window_seconds: int | None = None,
        batch_rows: int | None = None,
    ) -> None:
        """Mock save_state method."""
        if self._should_fail_save_state:
            raise Exception("ClickHouse save_state failed")
        if timestamp_progress is not None:
            self._state["timestamp_progress"] = timestamp_progress
        if timestamp_start is not None:
            self._state["timestamp_start"] = timestamp_start
        if timestamp_end is not None:
            self._state["timestamp_end"] = timestamp_end
        if batch_window_seconds is not None:
            self._state["batch_window_seconds"] = batch_window_seconds
        if batch_rows is not None:
            self._state["batch_rows"] = batch_rows


def _make_config(**kwargs: object) -> Config:
    """Create minimal valid Config for tests."""
    import tempfile

    etl_kwargs = kwargs.pop("etl", {})
    if "temp_dir" not in etl_kwargs:
        etl_kwargs["temp_dir"] = tempfile.gettempdir()

    return Config(
        prometheus=PrometheusConfig(
            url="http://prom:9090",
            **{k: v for k, v in kwargs.items() if k.startswith("prometheus_")},
        ),
        clickhouse=ClickHouseConfig(
            url="http://ch:8123",
            table_metrics="db.tbl",
            **{k: v for k, v in kwargs.items() if k.startswith("clickhouse_")},
        ),
        etl=EtlConfig(
            batch_window_size_seconds=300,  # overlap defaults to 0
            **etl_kwargs,
        ),
    )


def test_etl_job_run_once_success() -> None:
    """EtlJob.run_once should complete successfully in happy path."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Set initial state in ClickHouse
    ch._state["timestamp_progress"] = 1700000000
    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None

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
    )

    job.run_once()

    # Verify calls
    assert ch._state["timestamp_start"] is not None
    assert len(ch.inserts) == 1
    assert len(ch.inserts[0]) == 2  # Two data points
    assert ch._state["timestamp_progress"] is not None
    assert ch._state["batch_rows"] == 2.0


def test_etl_job_run_once_cannot_start_when_end_less_than_start() -> None:
    """EtlJob should raise RuntimeError when TimestampEnd < TimestampStart."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Set end < start (previous job still running)
    ch._state["timestamp_start"] = 1700000100
    ch._state["timestamp_end"] = 1700000000

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    with pytest.raises(RuntimeError, match="Job cannot start"):
        job.run_once()

    # Should not proceed - state should remain unchanged
    assert ch._state["timestamp_start"] == 1700000100  # Unchanged
    assert len(ch.inserts) == 0
    assert ch._state["timestamp_progress"] is None  # Not set yet


def test_etl_job_run_once_can_start_when_end_exists_but_start_missing() -> None:
    """EtlJob should start when TimestampEnd exists but TimestampStart missing."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # End exists but start doesn't - inconsistent state, but previous job finished
    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = 1700000000
    ch._state["timestamp_progress"] = 1700000000

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
    )

    job.run_once()

    # Should proceed despite inconsistent state (previous job finished)
    assert ch._state["timestamp_start"] is not None
    assert len(ch.inserts) == 1


def test_etl_job_run_once_cannot_start_when_start_exists_but_end_missing() -> None:
    """EtlJob should raise RuntimeError when TimestampStart exists but
    TimestampEnd missing."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Start exists but end doesn't - previous job is still running
    ch._state["timestamp_start"] = 1700000000
    ch._state["timestamp_end"] = None

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    with pytest.raises(RuntimeError, match="Job cannot start"):
        job.run_once()

    # Should not proceed - state should remain unchanged
    assert ch._state["timestamp_start"] == 1700000000  # Unchanged
    assert len(ch.inserts) == 0
    assert ch._state["timestamp_progress"] is None  # Not set yet


def test_etl_job_run_once_can_start_when_no_previous_run() -> None:
    """EtlJob should start when no previous run info exists."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # No previous run (empty state, but progress is required)
    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = 1700000000

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
    )

    job.run_once()

    # Should proceed
    assert ch._state["timestamp_start"] is not None
    assert len(ch.inserts) == 1


def test_etl_job_run_once_fails_on_mark_start_error() -> None:
    """EtlJob should raise RuntimeError if push_start fails."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    ch.set_should_fail_save_state(True)

    # Set initial state
    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = 1700000000

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    with pytest.raises(RuntimeError, match="Job cannot start"):
        job.run_once()

    # Should not proceed after failed start
    assert len(ch.inserts) == 0
    # Progress should remain unchanged (initial value)
    assert ch._state["timestamp_progress"] == 1700000000


def test_etl_job_run_once_fails_on_fetch_error() -> None:
    """EtlJob should raise exception if fetch_data fails."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Set initial state
    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = 1700000000

    # Make query_range raise exception
    def failing_query_range(*_: object, **__: object) -> dict[str, Any]:
        raise Exception("Prometheus query_range failed")

    prom.query_range = failing_query_range  # type: ignore[assignment]

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    with pytest.raises(Exception, match="Prometheus query_range failed"):
        job.run_once()

    # Should not write or push success
    assert len(ch.inserts) == 0
    # Progress should remain unchanged (initial value)
    assert ch._state["timestamp_progress"] == 1700000000


def test_etl_job_run_once_fails_on_write_error() -> None:
    """EtlJob should raise exception if write_rows fails."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Set initial state
    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = 1700000000
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
    )

    with pytest.raises(Exception, match="ClickHouse insert failed"):
        job.run_once()

    # Should not push success metrics
    # Progress should remain unchanged (initial value)
    assert ch._state["timestamp_progress"] == 1700000000


def test_etl_job_run_once_fails_when_progress_missing() -> None:
    """EtlJob should fail when TimestampProgress is not found in ClickHouse."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # No progress in state - should cause failure
    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = None  # Missing - should cause failure

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    with pytest.raises(ValueError, match="TimestampProgress.*not found"):
        job.run_once()

    # Should not proceed beyond progress loading
    # Note: _mark_start is called before _load_progress, so timestamp_start is pushed
    assert (
        ch._state["timestamp_start"] is not None
    )  # Start was marked before progress check
    assert len(ch.inserts) == 0  # But no data was processed
    assert ch._state["timestamp_progress"] is None  # And no success metrics


def test_etl_job_run_once_fails_on_push_success_error() -> None:
    """EtlJob should raise exception if push_success fails after successful write."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Set initial state
    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = 1700000000
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

    # Make save_state fail only on second call (after write)
    # First call (start) should succeed, second call (success) should fail
    original_save_state = ch.save_state

    call_count = 0

    def failing_save_state_after_start(*args: object, **kwargs: object) -> None:
        nonlocal call_count
        call_count += 1
        # First call is for start - allow it
        if call_count == 1:
            original_save_state(*args, **kwargs)
        else:
            # Second call is for success - fail it
            raise Exception("ClickHouse save_state failed")

    ch.save_state = failing_save_state_after_start  # type: ignore[assignment]

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    with pytest.raises(Exception, match="ClickHouse save_state failed"):
        job.run_once()

    # Data should be written to ClickHouse before save_state fails
    assert len(ch.inserts) == 1
    assert len(ch.inserts[0]) == 1
    # But save_state should not succeed - progress should remain unchanged
    assert ch._state["timestamp_progress"] == 1700000000


def test_etl_job_fetch_data_parses_prometheus_response() -> None:
    """EtlJob._fetch_data should correctly parse Prometheus query_range response."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Set initial state
    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = 1700000000

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
    """EtlJob._check_can_start should handle exceptions from ClickHouse get_state."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Make get_state raise exception
    ch.set_should_fail_get_state(True)

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    # Should return False and not raise exception
    # When get_state fails, _read_state_field raises exception,
    # which is caught in _check_can_start and returns False
    result = job._check_can_start()
    assert result is False


def test_etl_job_check_can_start_when_both_timestamps_exist_and_end_greater_than_start() -> (  # noqa: E501
    None
):
    """EtlJob._check_can_start should return True when both timestamps exist and end >= start."""  # noqa: E501
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Set both timestamps with end > start (previous job completed)
    ch.set_state(
        timestamp_start=1700000000,
        timestamp_end=1700000300,
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    # Should return True when end > start (previous job completed)
    result = job._check_can_start()
    assert result is True


def test_etl_job_check_can_start_when_both_timestamps_exist_and_end_equals_start() -> (
    None
):  # noqa: E501
    """EtlJob._check_can_start should return True when both timestamps exist and end == start."""  # noqa: E501
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Set both timestamps with end == start
    ch.set_state(
        timestamp_start=1700000000,
        timestamp_end=1700000000,
    )

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    # Should return True when end == start
    result = job._check_can_start()
    assert result is True


def test_etl_job_load_progress_handles_query_exception() -> None:
    """EtlJob._load_progress should raise exception when get_state fails."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Make get_state raise exception
    ch.set_should_fail_get_state(True)

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    # Mark start first (required for load_progress to be called)
    job._mark_start(1700000000)

    # Should raise exception
    with pytest.raises(Exception, match="ClickHouse get_state failed"):
        job._load_progress()


def test_etl_job_read_state_field_handles_missing_field() -> None:
    """EtlJob._read_state_field should return None when field is missing."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # State is empty (all fields are None)
    ch._state["timestamp_start"] = None

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    result = job._read_state_field("timestamp_start")
    assert result is None


def test_etl_job_read_state_field_returns_value_when_set() -> None:
    """EtlJob._read_state_field should return value when field is set."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Set state value
    ch._state["timestamp_start"] = 1700000000

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    result = job._read_state_field("timestamp_start")
    assert result == 1700000000


def test_etl_job_fetch_data_handles_invalid_value_pairs() -> None:
    """EtlJob._fetch_data should skip invalid value pairs in Prometheus response."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = 1700000000

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
    )

    job.run_once()

    # Should only process valid value pairs (2 out of 4)
    assert len(ch.inserts) == 1
    assert len(ch.inserts[0]) == 2


def test_etl_job_run_once_handles_empty_result_from_prometheus() -> None:
    """EtlJob should handle empty result from Prometheus gracefully."""
    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = 1700000000

    # Empty result from Prometheus
    prom.set_query_range_response({"status": "success", "data": {"result": []}})

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    job.run_once()

    # Should complete successfully even with empty result
    assert ch._state["timestamp_start"] is not None
    assert len(ch.inserts) == 0  # No rows to write
    assert ch._state["timestamp_progress"] is not None
    # Progress should still advance
    assert ch._state["timestamp_progress"] == 1700000300
    assert ch._state["batch_rows"] == 0.0


def test_etl_job_run_once_prevents_progress_from_going_into_future() -> None:
    """EtlJob should not allow progress to exceed current time."""
    import time

    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    # Set progress to a time very close to current time
    current_time = time.time()
    progress_in_future = current_time + 1000  # 1000 seconds in future

    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = progress_in_future

    # Empty result - no data in Prometheus for future window
    prom.set_query_range_response({"status": "success", "data": {"result": []}})

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    job.run_once()

    # Progress should be capped at current time, not go further into future
    assert ch._state["timestamp_progress"] is not None
    new_progress = ch._state["timestamp_progress"]
    # Should not exceed current time (allow small margin for execution time)
    assert new_progress <= time.time() + 1
    # Should not be progress_in_future + batch_window_size_seconds
    expected_future_progress = progress_in_future + config.etl.batch_window_size_seconds
    assert new_progress < expected_future_progress


def test_etl_job_calc_window_with_overlap() -> None:
    """EtlJob._calc_window should create overlap when configured."""
    # Use model_construct to bypass env_ignore_empty=True which ignores constructor args
    etl_config = EtlConfig.model_construct(
        batch_window_size_seconds=300, batch_window_overlap_seconds=20
    )
    config = Config(
        prometheus=PrometheusConfig(url="http://prom:9090"),
        clickhouse=ClickHouseConfig(url="http://ch:8123", table_metrics="db.tbl"),
        etl=etl_config,
    )
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    progress = 1000
    window_start, window_end = job._calc_window(progress)

    # Window should start at progress - overlap
    assert window_start == 980  # 1000 - 20
    # Window should end at progress + window_size
    assert window_end == 1300  # 1000 + 300
    # Window size should be window_size + overlap
    assert window_end - window_start == 320  # 300 + 20


def test_etl_job_calc_window_without_overlap() -> None:
    """EtlJob._calc_window should work correctly without overlap."""
    config = Config(
        prometheus=PrometheusConfig(url="http://prom:9090"),
        clickhouse=ClickHouseConfig(url="http://ch:8123", table_metrics="db.tbl"),
        etl=EtlConfig(batch_window_size_seconds=300),  # overlap defaults to 0
    )
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    job = EtlJob(
        config=config,
        prometheus_client=prom,
        clickhouse_client=ch,
    )

    progress = 1000
    window_start, window_end = job._calc_window(progress)

    # Window should start at progress (no overlap)
    assert window_start == 1000
    # Window should end at progress + window_size
    assert window_end == 1300  # 1000 + 300
    # Window size should be exactly window_size
    assert window_end - window_start == 300


def test_etl_job_fetch_data_handles_file_write_error() -> None:
    """EtlJob._fetch_data should handle file write errors and clean up file."""
    from unittest.mock import patch

    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = 1700000000

    # Mock query_range response with data
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
    )

    # Mock os.fdopen to raise exception during file write
    with patch("etl_job.os.fdopen", side_effect=OSError("Disk full")):
        with pytest.raises(OSError, match="Disk full"):
            job._fetch_data(1700000000, 1700000300)


def test_etl_job_fetch_data_handles_file_cleanup_error_on_write_error() -> None:
    """EtlJob._fetch_data should handle cleanup errors when file write fails."""
    from unittest.mock import MagicMock, patch

    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = 1700000000

    # Mock query_range response with data
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
    )

    # Mock file object that raises error on write
    mock_file = MagicMock()
    mock_file.write.side_effect = OSError("Disk full")
    mock_file.__enter__ = MagicMock(return_value=mock_file)
    mock_file.__exit__ = MagicMock(return_value=False)

    # Mock tempfile.mkstemp to return a file descriptor and path
    import tempfile

    temp_dir = tempfile.gettempdir()
    fd = 123  # Dummy file descriptor
    file_path = f"{temp_dir}/etl_batch_test.jsonl"

    # Mock os.fdopen to return file that fails on write
    # and os.unlink to fail during cleanup (to test nested exception handling)
    with (
        patch("etl_job.tempfile.mkstemp", return_value=(fd, file_path)),
        patch("etl_job.os.fdopen", return_value=mock_file),
        patch("etl_job.os.unlink", side_effect=OSError("Permission denied")),
    ):
        # Should raise original write error, not cleanup error
        # Cleanup error should be silently ignored (lines 441-443)
        with pytest.raises(OSError, match="Disk full"):
            job._fetch_data(1700000000, 1700000300)


def test_etl_job_run_once_handles_file_cleanup_error() -> None:
    """EtlJob.run_once should handle file cleanup errors gracefully."""
    from unittest.mock import patch

    config = _make_config()
    prom = DummyPromClient()
    ch = DummyClickHouseClient()

    ch._state["timestamp_start"] = None
    ch._state["timestamp_end"] = None
    ch._state["timestamp_progress"] = 1700000000

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
    )

    # Mock os.unlink to raise exception during cleanup
    # This should not prevent job from completing successfully
    with patch("etl_job.os.unlink", side_effect=OSError("Permission denied")):
        # Job should complete successfully despite cleanup error
        job.run_once()

    # Should have written data and pushed success metrics
    assert len(ch.inserts) == 1
    assert ch._state["timestamp_progress"] is not None
