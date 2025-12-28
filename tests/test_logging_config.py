"""
Tests for logging configuration.
"""

import io
import json
import logging
from datetime import datetime, timezone

from logging_config import getLogger


def _parse_iso8601_utc(timestamp_str: str) -> datetime:
    """Parse ISO 8601 timestamp with UTC timezone.

    ECS formatter uses ISO 8601 format with 'Z' suffix for UTC.
    Also supports +00:00 format for compatibility.

    Args:
        timestamp_str: ISO 8601 formatted string (with Z or +00:00)

    Returns:
        datetime object with UTC timezone

    Raises:
        AssertionError: If timestamp does not conform to ISO 8601 format
    """
    # ECS formatter uses 'Z' suffix for UTC, convert to +00:00 for fromisoformat
    if timestamp_str.endswith("Z"):
        timestamp_str = timestamp_str[:-1] + "+00:00"

    try:
        parsed_time = datetime.fromisoformat(timestamp_str)
    except ValueError as exc:
        raise AssertionError(
            f"Timestamp does not conform to ISO 8601 format: {timestamp_str}"
        ) from exc

    # Verify it's UTC timezone
    if parsed_time.tzinfo != timezone.utc:
        raise AssertionError(
            f"Timestamp should be in UTC timezone, got: {parsed_time.tzinfo}"
        )

    return parsed_time


def test_logging_format_ecs_json() -> None:
    """Logger should output ECS-formatted JSON logs with UTC timestamps."""
    logger = getLogger("test_logging_format")

    # Create a string buffer to capture output
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.INFO)

    # Get the formatter from logger's existing handlers
    # The logger already has handlers from getLogger, so we'll reuse the formatter
    existing_formatter = None
    if logger.handlers:
        existing_formatter = logger.handlers[0].formatter
    if existing_formatter:
        handler.setFormatter(existing_formatter)

    logger.addHandler(handler)

    try:
        # Log a test message
        logger.info("Test message")

        # Get the output
        output = stream.getvalue().strip()

        # ECS formatter outputs JSON, parse it
        log_data = json.loads(output)

        # Verify ECS structure
        assert "@timestamp" in log_data, "ECS log should have @timestamp field"
        assert "log.level" in log_data, "ECS log should have log.level field"
        assert "message" in log_data, "ECS log should have message field"
        assert log_data["message"] == "Test message"

        # Extract and validate timestamp
        timestamp_str = log_data["@timestamp"]
        parsed_time = _parse_iso8601_utc(timestamp_str)

        # Verify it's close to current UTC time (within reasonable bounds)
        now_utc = datetime.now(timezone.utc)
        time_diff = abs((now_utc - parsed_time).total_seconds())
        assert (
            time_diff < 10
        ), f"Timestamp should be close to current UTC time, diff: {time_diff}s"
    finally:
        logger.removeHandler(handler)


def test_logging_uses_utc_time() -> None:
    """Logger should use UTC time (not local time) in ECS format."""
    logger = getLogger("test_utc_time")

    # Create a string buffer to capture output
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.INFO)

    # Get the formatter from logger's existing handlers
    existing_formatter = None
    if logger.handlers:
        existing_formatter = logger.handlers[0].formatter
    if existing_formatter:
        handler.setFormatter(existing_formatter)

    logger.addHandler(handler)

    try:
        # Log a test message
        logger.info("Test UTC time")

        # Get the output
        output = stream.getvalue().strip()

        # ECS formatter outputs JSON, parse it
        log_data = json.loads(output)

        # Extract timestamp from ECS format
        timestamp_str = log_data["@timestamp"]
        log_time_utc = _parse_iso8601_utc(timestamp_str)

        # Current UTC time
        now_utc = datetime.now(timezone.utc)

        # Timestamp should be within last 10 seconds (allowing for test execution time)
        time_diff = abs((now_utc - log_time_utc).total_seconds())
        assert (
            time_diff < 10
        ), f"Log time should be close to current UTC time, diff: {time_diff}s"
    finally:
        logger.removeHandler(handler)


def test_logging_resolve_log_level_with_int() -> None:
    """_resolve_log_level should handle int level values."""
    from logging_config import _resolve_log_level

    # Test with int level
    result = _resolve_log_level(logging.INFO)
    assert result == logging.INFO

    result = _resolve_log_level(logging.DEBUG)
    assert result == logging.DEBUG

    result = _resolve_log_level(logging.ERROR)
    assert result == logging.ERROR


def test_logging_get_logger_with_existing_logger() -> None:
    """getLogger should reuse existing logger configuration."""
    # Get logger first time - should create handlers
    logger1 = getLogger("test_reuse_logger")
    initial_handlers_count = len(logger1.handlers)
    assert initial_handlers_count > 0

    # Get same logger second time - should reuse existing handlers
    logger2 = getLogger("test_reuse_logger")
    assert logger2 is logger1
    assert len(logger2.handlers) == initial_handlers_count
