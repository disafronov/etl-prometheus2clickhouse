## [1.0.0-rc.10](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.0.0-rc.9...v1.0.0-rc.10) (2025-12-25)

### Features

* introduce batch window overlap configuration for ETL job ([3c5f7be](https://github.com/disafronov/etl-prometheus2clickhouse/commit/3c5f7be0773156b188b73c6c1330659cd4a367dc))

### Bug Fixes

* correct variable reference in ETL progress logging within Docker Compose ([f00c6d8](https://github.com/disafronov/etl-prometheus2clickhouse/commit/f00c6d8363b84540671e7114c16cf37f1e485ebf))
* improve progress calculation and logging in ETL job ([d2405c1](https://github.com/disafronov/etl-prometheus2clickhouse/commit/d2405c186b18a8e94942fe58f94aabacbf2d70dc))

## [1.0.0-rc.9](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.0.0-rc.8...v1.0.0-rc.9) (2025-12-25)

### Features

* enhance logging for ETL job data fetching ([ddaeb15](https://github.com/disafronov/etl-prometheus2clickhouse/commit/ddaeb15cae34815ee6d109ef9f0c4498d746c1b5))
* enhance logging throughout ETL job process ([a86be1f](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a86be1f3085aaa4a84fde508f9967da297bf6d5f))

### Bug Fixes

* ensure timestamp metrics are integers in PushGatewayClient ([a84daf2](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a84daf2c1f4e3468ed1572e5f7ce6156471f2741))
* prevent future timestamp errors in ETL job progress calculation ([c9d3488](https://github.com/disafronov/etl-prometheus2clickhouse/commit/c9d34882ffd45cbef9bca694d73d2873b29c1efb))

## [1.0.0-rc.8](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.0.0-rc.7...v1.0.0-rc.8) (2025-12-25)

### Features

* add Docker Compose configuration for ClickHouse, Prometheus, and PushGateway ([2cf507f](https://github.com/disafronov/etl-prometheus2clickhouse/commit/2cf507faf9dfdab81d20546bdd1b3a58ffca4697))

### Bug Fixes

* update logging level for job status checks in ETL job ([064b950](https://github.com/disafronov/etl-prometheus2clickhouse/commit/064b95009273a3fd21801f56ed0cb95be5067800))

## [1.0.0-rc.7](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.0.0-rc.6...v1.0.0-rc.7) (2025-12-25)

### Features

* enhance error handling for invalid value pairs in ETL job ([5a18b16](https://github.com/disafronov/etl-prometheus2clickhouse/commit/5a18b16559fee9fc8d56196e4d338d4017e4b667))

## [1.0.0-rc.6](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.0.0-rc.5...v1.0.0-rc.6) (2025-12-25)

### Features

* enhance password normalization in PushGateway configuration ([4ddef6d](https://github.com/disafronov/etl-prometheus2clickhouse/commit/4ddef6d320e707555857151b24e849e83b14306f))
* normalize ClickHouse password handling in configuration ([b53ca41](https://github.com/disafronov/etl-prometheus2clickhouse/commit/b53ca4107ed31d4c278cfd6fd96286178d0f3a74))
* normalize password handling in Prometheus configuration ([c373365](https://github.com/disafronov/etl-prometheus2clickhouse/commit/c3733652fd5b7626125e8589dd2f352927de4270))

## [1.0.0-rc.5](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.0.0-rc.4...v1.0.0-rc.5) (2025-12-25)

## [1.0.0-rc.4](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.0.0-rc.3...v1.0.0-rc.4) (2025-12-25)

### Bug Fixes

* ClickHouseClient URL handling ([fc9cc99](https://github.com/disafronov/etl-prometheus2clickhouse/commit/fc9cc99d76fa1191e07719565f6f08994e0d9505))

## [1.0.0-rc.3](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.0.0-rc.2...v1.0.0-rc.3) (2025-12-25)

## [1.0.0-rc.2](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.0.0-rc.1...v1.0.0-rc.2) (2025-12-24)

### Features

* add TLS verification control for ClickHouse and PushGateway clients ([bd97e16](https://github.com/disafronov/etl-prometheus2clickhouse/commit/bd97e1613de8afefc5eac4639925590630cf9497))

## 1.0.0-rc.1 (2025-12-24)

### Features

* initial ([f2777ae](https://github.com/disafronov/etl-prometheus2clickhouse/commit/f2777ae1331b617379a31b4f2477f0287f07f89c))
