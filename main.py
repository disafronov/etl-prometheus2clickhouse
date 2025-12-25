#!/usr/bin/env python3
"""
Application entry point.
"""

from __future__ import annotations

import sys

from clickhouse_client import ClickHouseClient
from config import Config, load_config
from etl_job import EtlJob
from logging_config import getLogger, set_all_loggers_level
from prometheus_client import PrometheusClient

logger = getLogger(__name__)


def main() -> None:
    """Main application entry point.

    Initializes all clients, creates ETL job, and runs single iteration.
    All errors are caught, logged with structured logging, and cause
    exit with code 1 for proper monitoring integration.

    Exits:
        0: Success
        1: Error occurred (logged before exit)
    """
    try:
        config: Config = load_config()

        # Update logger level from config after configuration is loaded
        set_all_loggers_level(config.etl.log_level)

        prom_client = PrometheusClient(config.prometheus)
        ch_client = ClickHouseClient(config.clickhouse)

        job = EtlJob(
            config=config,
            prometheus_client=prom_client,
            clickhouse_client=ch_client,
        )
        job.run_once()
    except Exception as exc:
        logger.error(
            "Application error occurred",
            extra={
                "main.application_error.error": str(exc),
                "main.application_error.error_type": type(exc).__name__,
                "main.application_error.message": (
                    f"Unexpected error occurred: {type(exc).__name__}: {exc}"
                ),
            },
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
