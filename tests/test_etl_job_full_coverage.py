import io
import json
import os
import tempfile

from etl_job import EtlJob
from tests.test_etl_job import DummyClickHouseClient, DummyPromClient, _make_config


def test_format_float_special_values():
    # NaN should be formatted as 'nan' (lowercase) for ClickHouse
    assert EtlJob._format_float(float("nan")) == "nan"
    # Positive infinity should be formatted as 'inf'
    assert EtlJob._format_float(float("inf")) == "inf"
    # Negative infinity should be formatted as '-inf'
    assert EtlJob._format_float(float("-inf")) == "-inf"
    # Regular float should be formatted without scientific notation
    assert EtlJob._format_float(1234.5678) == "1234.5678"


def test_stream_parse_handles_all_value_variants():
    # Prepare a Prometheus-like JSON containing various value types
    response = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {"__name__": "varied_metric"},
                    "values": [
                        [1600000000, "42"],  # numeric string
                        [1600000060, "NaN"],  # NaN string
                        [1600000120, "Inf"],  # +Inf string
                        [1600000180, "-Inf"],  # -Inf string
                        [1600000240, "invalid"],  # invalid string (should be skipped)
                        [1600000300, 123.456],  # raw number (int/float)
                    ],
                }
            ]
        },
    }
    # Write JSON to a temporary file
    tmp_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    try:
        json.dump(response, tmp_file)
        tmp_file.close()
        with open(tmp_file.name, "rb") as f:
            job = EtlJob(
                config=_make_config(),
                prometheus_client=DummyPromClient(),
                clickhouse_client=DummyClickHouseClient(),
            )
            rows, series, skipped = job._stream_parse_prometheus_response(
                f, io.StringIO()
            )
            # Expected: 5 valid rows (all except the invalid string)
            assert rows == 5
            # One series processed
            assert series == 1
            # One skipped pair (the invalid string)
            assert skipped == 1
    finally:
        os.unlink(tmp_file.name)


def _run_parse(response: dict) -> tuple[int, int, int]:
    tmp_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    try:
        json.dump(response, tmp_file)
        tmp_file.close()
        with open(tmp_file.name, "rb") as f:
            job = EtlJob(
                config=_make_config(),
                prometheus_client=DummyPromClient(),
                clickhouse_client=DummyClickHouseClient(),
            )
            return job._stream_parse_prometheus_response(f, io.StringIO())
    finally:
        os.unlink(tmp_file.name)


def test_stream_parse_metric_label_non_string_value():
    # Branch 604->590: metric label has a numeric (non-string) JSON value.
    # ijson emits event="number" for the label — ignored (only "string" is used).
    response = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {"__name__": "test_metric", "code": 200},
                    "values": [[1600000000, "1.0"]],
                }
            ]
        },
    }
    rows, series, skipped = _run_parse(response)
    assert series == 1
    assert skipped == 0
    assert rows == 1


def test_stream_parse_value_pair_null_element():
    # Branch 664->590: value-pair element has event="null" (not "number"/"string").
    # The pair never reaches length 2 and is silently dropped.
    response = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {"__name__": "test_metric"},
                    "values": [[1600000000, None]],
                }
            ]
        },
    }
    rows, series, skipped = _run_parse(response)
    assert series == 1
    assert skipped == 0
    assert rows == 0


def test_stream_parse_values_array_contains_scalars():
    # Branch 719->590: values array contains scalar items instead of nested arrays.
    # ijson emits prefix "data.result.item.values.item" with event "number"/"string",
    # which matches none of the three handled sub-conditions while in_values_array is
    # True. The scalars are silently ignored.
    response = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {"__name__": "test_metric"},
                    "values": [1600000000, "42"],
                }
            ]
        },
    }
    rows, series, skipped = _run_parse(response)
    assert series == 1
    assert skipped == 0
    assert rows == 0
