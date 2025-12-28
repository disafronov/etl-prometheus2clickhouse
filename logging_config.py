#!/usr/bin/env python3
"""
Logging configuration using SchemaLogger and ECS formatter.

This module configures a global logging setup for the application:
- SchemaLogger is used as the base logger class.
- ECS (Elastic Common Schema) formatter is used for structured JSON logging.
- Non-error messages (DEBUG, INFO, WARNING) are sent to stdout.
- Error messages (ERROR and above) are sent to stderr.
All timestamps are formatted in UTC timezone by ECS formatter.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from functools import lru_cache

import ecs_logging
from logging_objects_with_schema import SchemaLogger

# Configure SchemaLogger as the default logger class
logging.setLoggerClass(SchemaLogger)

# Reuse single ECS formatter instance for all loggers
_ecs_formatter = ecs_logging.StdlibFormatter()


def _filter_non_error(record: logging.LogRecord) -> bool:
    """Allow only records below ERROR level."""
    return record.levelno < logging.ERROR


@lru_cache(maxsize=32)
def _resolve_log_level(level: str | int) -> int:
    """Resolve log level from string or int to logging level constant."""
    if isinstance(level, str):
        return getattr(logging, level.upper(), logging.INFO)
    return int(level)


def _is_logger_configured(logger: logging.Logger) -> bool:
    """Check if logger already has handlers configured."""
    return bool(logger.handlers)


@lru_cache(maxsize=1)
def _get_log_level() -> str:
    """Get LOG_LEVEL from environment with caching.

    This function is used for backward compatibility when getLogger is called
    before configuration is loaded. In normal operation, log_level should be
    passed directly to getLogger().
    """
    return os.getenv("LOG_LEVEL", "INFO")


def _make_logger(name: str, level: str | int) -> logging.Logger:
    """Create and configure a logger with stdout/stderr handlers and ECS formatter.

    Stdout handler:
        - Receives messages below ERROR level.
    Stderr handler:
        - Receives ERROR and above.
    All logs are formatted as JSON using ECS (Elastic Common Schema) format.
    Timestamps are automatically formatted in UTC by ECS formatter.
    """
    logger = logging.getLogger(name)

    if not _is_logger_configured(logger):
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        stdout_handler.addFilter(_filter_non_error)
        stdout_handler.setFormatter(_ecs_formatter)

        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.ERROR)
        stderr_handler.setFormatter(_ecs_formatter)

        logger.addHandler(stdout_handler)
        logger.addHandler(stderr_handler)
        logger.propagate = False

    resolved_level = _resolve_log_level(level)
    if logger.level != resolved_level:
        logger.setLevel(resolved_level)

    return logger


def getLogger(name: str, log_level: str | None = None) -> logging.Logger:
    """Return configured logger instance with level from config or env.

    Args:
        name: Logger name (typically __name__)
        log_level: Optional log level from config. If None, reads from
            LOG_LEVEL environment variable (for backward compatibility).

    Returns:
        Configured logger instance
    """
    level = log_level if log_level is not None else _get_log_level()
    return _make_logger(name, level)


def set_all_loggers_level(level: str | int) -> None:
    """Set logging level for all existing loggers.

    Updates level for root logger and all named loggers. Used to apply
    log level from configuration after it's loaded.

    Args:
        level: Log level as string (e.g., "INFO") or int (logging constant)
    """
    resolved_level = _resolve_log_level(level)
    logging.getLogger().setLevel(resolved_level)
    for logger_name in logging.Logger.manager.loggerDict:
        existing_logger = logging.getLogger(logger_name)
        if existing_logger.level != resolved_level:
            existing_logger.setLevel(resolved_level)


def format_timestamp_with_utc(timestamp: int) -> str:
    """Format Unix timestamp with UTC expansion in parentheses.

    Formats timestamp as: "timestamp (YYYY-MM-DDTHH:MM:SS+00:00)"
    Useful for log messages where numeric timestamps need human-readable UTC expansion.

    Args:
        timestamp: Unix timestamp in seconds (int)

    Returns:
        Formatted string with timestamp and UTC expansion
    """
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    utc_str = dt.isoformat(timespec="seconds")
    return f"{timestamp} ({utc_str})"
