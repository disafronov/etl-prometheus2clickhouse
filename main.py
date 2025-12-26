#!/usr/bin/env python3
"""
Application entry point.
"""

from __future__ import annotations

import sys
from pathlib import Path

import tomli

from clickhouse_client import ClickHouseClient
from config import Config, load_config
from etl_job import EtlJob
from logging_config import getLogger, set_all_loggers_level
from prometheus_client import PrometheusClient

logger = getLogger(__name__)


def _get_project_info() -> tuple[str, str, str]:
    """Read project name, version, and authors from pyproject.toml.

    Returns:
        Tuple of (project_name, version, authors) from pyproject.toml.
        Authors are joined with comma if multiple.
    """
    pyproject_path = Path(__file__).parent / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        data = tomli.load(f)
    project_name: str = data["project"]["name"]
    version: str = data["project"]["version"]
    # Get all authors' names, joined with comma, or "Unknown" if not found
    authors = data.get("project", {}).get("authors", [])
    authors_str: str = (
        ", ".join(author["name"] for author in authors) if authors else "Unknown"
    )
    return project_name, version, authors_str


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
        project_name, version, author = _get_project_info()
        logger.info(f"Starting {project_name} {version} by {author}")

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
        error_msg = f"Application error occurred: {type(exc).__name__}: {exc}"
        logger.error(
            error_msg,
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
