#!/usr/bin/env python3
"""
Configuration loading and validation.

All connection settings are read from environment variables. Business state
timestamps are stored in Prometheus metrics and are not part of this config.
"""

from __future__ import annotations

from pydantic import AliasChoices, BaseModel, Field, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from logging_config import getLogger

logger = getLogger(__name__)


class PrometheusConfig(BaseSettings):
    """Prometheus or Mimir connection configuration.

    Configuration for connecting to Prometheus-compatible API. Business state
    (timestamps) is stored in Prometheus metrics, not in this config object.
    """

    model_config = SettingsConfigDict(
        env_prefix="PROMETHEUS_",
        case_sensitive=False,
        extra="ignore",
        env_ignore_empty=True,
    )

    url: str = Field(..., description="Base URL of Prometheus/Mimir")
    user: str | None = Field(
        default=None,
        description="Optional basic auth username",
    )
    password: str | None = Field(
        default=None,
        description="Optional basic auth password",
    )
    insecure: bool = Field(
        default=False,
        description="Disable TLS verification when true",
    )
    timeout: int = Field(
        default=10,
        description="HTTP request timeout in seconds",
    )
    query_step_seconds: int = Field(
        default=15,
        description=(
            "Step resolution for Prometheus query_range in seconds. "
            "Should match scrape_interval to get all data points"
        ),
    )

    @model_validator(mode="after")
    def normalize_password(self) -> PrometheusConfig:
        """Normalize password: if user is specified but password is None,
        convert password to empty string.

        This handles the case when PROMETHEUS_PASSWORD is set to empty string
        in environment variables. With env_ignore_empty=True, empty strings
        are converted to None, but HTTP Basic Auth requires explicit
        authentication even with empty password when user is specified.

        Returns:
            Self with normalized password field
        """
        if self.user is not None and self.password is None:
            # This is not a hardcoded password, but normalization of empty
            # password value. Empty string is required for HTTP Basic Auth
            # when password is empty but user is specified.
            self.password = ""  # nosec B105
        return self


class ClickHouseConfig(BaseSettings):
    """ClickHouse connection configuration.

    Configuration for ClickHouse HTTP interface. Used for batch inserts
    of processed metric data.
    """

    model_config = SettingsConfigDict(
        env_prefix="CLICKHOUSE_",
        case_sensitive=False,
        extra="ignore",
        env_ignore_empty=True,
    )

    url: str = Field(..., description="Base URL of ClickHouse HTTP interface")
    user: str | None = Field(default=None, description="Optional ClickHouse user")
    password: str | None = Field(
        default=None,
        description="Optional ClickHouse password",
    )
    connect_timeout: int = Field(
        default=10,
        description="HTTP connection timeout in seconds for ClickHouse client",
    )
    send_receive_timeout: int = Field(
        default=300,
        description=(
            "HTTP send/receive timeout in seconds for ClickHouse operations. "
            "Used for insert operations. Default is 300 seconds (5 minutes)."
        ),
    )
    insecure: bool = Field(
        default=False,
        description="Disable TLS verification when true",
    )
    table: str = Field(..., description="Target table name for inserts")

    @model_validator(mode="after")
    def normalize_password(self) -> ClickHouseConfig:
        """Normalize password: if user is specified but password is None,
        convert password to empty string.

        This handles the case when CLICKHOUSE_PASSWORD is set to empty string
        in environment variables. With env_ignore_empty=True, empty strings
        are converted to None, but ClickHouse requires explicit authentication
        even with empty password when user is specified.

        Returns:
            Self with normalized password field
        """
        if self.user is not None and self.password is None:
            # This is not a hardcoded password, but normalization of empty
            # password value. Empty string is required for ClickHouse
            # authentication when password is empty but user is specified.
            self.password = ""  # nosec B105
        return self


class PushGatewayConfig(BaseSettings):
    """PushGateway connection configuration.

    Configuration for Prometheus PushGateway. Used only for writing job
    state metrics. Job never reads from PushGateway (reads from Prometheus).
    """

    model_config = SettingsConfigDict(
        env_prefix="PUSHGATEWAY_",
        case_sensitive=False,
        extra="ignore",
        env_ignore_empty=True,
    )

    url: str = Field(..., description="Base URL of PushGateway")
    token: str | None = Field(
        default=None,
        description="Optional bearer token for PushGateway authentication",
    )
    user: str | None = Field(
        default=None,
        description="Optional basic auth username for PushGateway",
    )
    password: str | None = Field(
        default=None,
        validation_alias=AliasChoices("PUSHGATEWAY_PASS", "password"),
        description="Optional basic auth password for PushGateway",
    )
    timeout: int = Field(
        default=10,
        description="HTTP request timeout in seconds for PushGateway",
    )
    insecure: bool = Field(
        default=False,
        description="Disable TLS verification when true",
    )
    job: str = Field(..., description="PushGateway job name")
    instance: str = Field(..., description="PushGateway instance name")


class EtlConfig(BaseSettings):
    """ETL job configuration options.

    Controls ETL processing behavior. Batch window size determines how much
    data is processed in each iteration.
    """

    model_config = SettingsConfigDict(
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
        env_ignore_empty=True,
    )

    batch_window_seconds: int = Field(
        default=300,
        validation_alias="BATCH_WINDOW_SECONDS",
        description="Processing window size in seconds for each batch",
    )
    log_level: str = Field(
        default="INFO",
        validation_alias="LOG_LEVEL",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )


class Config(BaseModel):
    """Top-level application configuration."""

    prometheus: PrometheusConfig
    clickhouse: ClickHouseConfig
    pushgateway: PushGatewayConfig
    etl: EtlConfig


def load_config() -> Config:
    """Load configuration from environment variables.

    Reads all configuration from environment variables and validates using
    Pydantic Settings. Each nested config uses its own env_prefix to read
    from environment variables automatically. Business state (job timestamps)
    is not part of config - it's stored in Prometheus metrics and read at runtime.

    Returns:
        Validated Config instance with all connection settings

    Raises:
        ValueError: If configuration is invalid or required variables are missing.
    """
    try:
        # Initialize nested configs separately so they can use their env_prefix
        # BaseSettings reads from environment variables automatically, so no
        # arguments needed. Mypy doesn't understand that BaseSettings can be
        # instantiated without arguments when fields are read from environment
        # variables via env_prefix
        prometheus = PrometheusConfig()  # type: ignore[call-arg]
        clickhouse = ClickHouseConfig()  # type: ignore[call-arg]
        pushgateway = PushGatewayConfig()  # type: ignore[call-arg]
        etl = EtlConfig()

        return Config(
            prometheus=prometheus,
            clickhouse=clickhouse,
            pushgateway=pushgateway,
            etl=etl,
        )
    except ValidationError as exc:
        # Log configuration error details according to schema
        logger.error(
            "Configuration validation failed",
            extra={
                "config.config_type_error.expected": "Config",
                "config.config_type_error.actual": "invalid",
                "config.config_type_error.path": "env",
            },
        )
        raise ValueError(f"Configuration validation failed: {exc}") from exc
