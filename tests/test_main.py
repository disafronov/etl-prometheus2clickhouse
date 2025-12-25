"""
Basic tests for main entry point.
"""

from unittest.mock import Mock, patch

from main import main


@patch("main.EtlJob")
@patch("main.PushGatewayClient")
@patch("main.ClickHouseClient")
@patch("main.PrometheusClient")
@patch("main.load_config")
def test_main_success(
    mock_load_config: Mock,
    mock_prom_client: Mock,
    mock_ch_client: Mock,
    mock_pg_client: Mock,
    mock_etl_job: Mock,
) -> None:
    """main() should complete successfully in happy path."""
    from config import (
        ClickHouseConfig,
        Config,
        EtlConfig,
        PrometheusConfig,
        PushGatewayConfig,
    )

    mock_config = Config(
        prometheus=PrometheusConfig(url="http://prom:9090"),
        clickhouse=ClickHouseConfig(url="http://ch:8123", table="db.tbl"),
        pushgateway=PushGatewayConfig(url="http://pg:9091", job="job", instance="inst"),
        etl=EtlConfig(batch_window_size_seconds=300),  # overlap defaults to 0
    )
    mock_load_config.return_value = mock_config

    mock_prom = Mock()
    mock_prom_client.return_value = mock_prom

    mock_ch = Mock()
    mock_ch_client.return_value = mock_ch

    mock_pg = Mock()
    mock_pg_client.return_value = mock_pg

    mock_job = Mock()
    mock_etl_job.return_value = mock_job

    main()

    mock_load_config.assert_called_once()
    mock_prom_client.assert_called_once_with(mock_config.prometheus)
    mock_ch_client.assert_called_once_with(mock_config.clickhouse)
    mock_pg_client.assert_called_once_with(mock_config.pushgateway)
    mock_etl_job.assert_called_once_with(
        config=mock_config,
        prometheus_client=mock_prom,
        clickhouse_client=mock_ch,
        pushgateway_client=mock_pg,
    )
    mock_job.run_once.assert_called_once()


@patch("main.EtlJob")
@patch("main.PushGatewayClient")
@patch("main.ClickHouseClient")
@patch("main.PrometheusClient")
@patch("main.load_config")
@patch("main.sys.exit")
def test_main_config_error(
    mock_exit: Mock,
    mock_load_config: Mock,
    mock_prom_client: Mock,
    mock_ch_client: Mock,
    mock_pg_client: Mock,
    mock_etl_job: Mock,
) -> None:
    """main() should exit with code 1 on configuration error."""
    mock_load_config.side_effect = ValueError("Config error")

    main()

    mock_exit.assert_called_once_with(1)
    mock_prom_client.assert_not_called()


@patch("main.EtlJob")
@patch("main.PushGatewayClient")
@patch("main.ClickHouseClient")
@patch("main.PrometheusClient")
@patch("main.load_config")
@patch("main.sys.exit")
def test_main_etl_job_error(
    mock_exit: Mock,
    mock_load_config: Mock,
    mock_prom_client: Mock,
    mock_ch_client: Mock,
    mock_pg_client: Mock,
    mock_etl_job: Mock,
) -> None:
    """main() should exit with code 1 on ETL job error."""
    from config import Config

    mock_config = Mock(spec=Config)
    mock_load_config.return_value = mock_config

    mock_prom = Mock()
    mock_prom_client.return_value = mock_prom

    mock_ch = Mock()
    mock_ch_client.return_value = mock_ch

    mock_pg = Mock()
    mock_pg_client.return_value = mock_pg

    mock_job = Mock()
    mock_job.run_once.side_effect = Exception("ETL job failed")
    mock_etl_job.return_value = mock_job

    main()

    mock_exit.assert_called_once_with(1)
