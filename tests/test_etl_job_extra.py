import io
import json
import os
import tempfile

from etl_job import EtlJob
from tests.test_etl_job import DummyClickHouseClient, DummyPromClient, _make_config


def test_escape_tabseparated_chars():
    raw = "back\\slash\tnewline\nend"
    escaped = EtlJob._escape_tabseparated_chars(raw)
    assert escaped == "back\\\\slash\\tnewline\\nend"


def test_format_clickhouse_array():
    arr = ["a'b", "c\\d", "e\tf", "g\nh"]
    formatted = EtlJob._format_clickhouse_array(arr)
    # Expected ClickHouse array string with escaped characters
    expected = "['a\\'b','c\\\\d','e\\tf','g\\nh']"
    assert formatted == expected


def test_create_temp_file_creates_file(tmp_path):
    # Build a minimal config with temp_dir pointing to tmp_path
    config = _make_config(etl={"temp_dir": str(tmp_path)})
    job = EtlJob(
        config=config,
        prometheus_client=DummyPromClient(),
        clickhouse_client=DummyClickHouseClient(),
    )
    fd, path = job._create_temp_file(prefix="test_", suffix=".tmp")
    os.close(fd)
    # File should exist and be writable
    assert os.path.isfile(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write("test")
    with open(path, "r", encoding="utf-8") as f:
        assert f.read() == "test"


def test_stream_parse_skips_invalid_value_and_counts_skipped():
    # Prepare a Prometheus-like JSON with an invalid string value
    response = {
        "status": "success",
        "data": {
            "result": [
                {
                    "metric": {"__name__": "test_metric"},
                    "values": [
                        [1600000000, "invalid"],  # should be skipped
                        [1600000060, "42"],  # valid value
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
            # One valid row should be written, one series processed, one skipped pair
            assert rows == 1
            assert series == 1
            assert skipped == 1
    finally:
        os.unlink(tmp_file.name)
