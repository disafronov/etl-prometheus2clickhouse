## [2.0.0-rc.3](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v2.0.0-rc.2...v2.0.0-rc.3) (2025-12-25)

### Bug Fixes

* Enhance error logging in ClickHouseClient and main application ([b288aba](https://github.com/disafronov/etl-prometheus2clickhouse/commit/b288aba2e1403e576218866a68e69cf151dc2e77))

## [2.0.0-rc.2](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v2.0.0-rc.1...v2.0.0-rc.2) (2025-12-25)

### Features

* Add validation for batch window size and overlap in EtlConfig ([c383941](https://github.com/disafronov/etl-prometheus2clickhouse/commit/c38394165562857bf7b696fa5f94c02f19638dda))

## [2.0.0-rc.1](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.1.0-rc.2...v2.0.0-rc.1) (2025-12-25)

### ⚠ BREAKING CHANGES

* Update logging schema to version 3 and change data types for window start and end fields
* Removed PushGatewayClient and related configurations, streamlining the ETL process. Timestamp parameters now use int instead of float.

Signed-off-by: Dmitrii Safronov <zimniy@cyberbrain.cc>

### Features

* Add integer validation for SQL insertion in ClickHouseClient ([a775209](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a775209c263c7c18e0ab60f3d7dedb18fd295df0))
* Add table name validation in ClickHouseClient ([cfd61d8](https://github.com/disafronov/etl-prometheus2clickhouse/commit/cfd61d8004e78cb1cb8d152cdcfc374161d8c0a6))
* Add warning for large files in insert_from_file() fallback ([d1b6053](https://github.com/disafronov/etl-prometheus2clickhouse/commit/d1b60533f3c727635fca8207f3629e17fcb0a481))
* Enhance error handling in EtlJob for job start conditions ([29b08db](https://github.com/disafronov/etl-prometheus2clickhouse/commit/29b08db75b59d1c116bb2fecdc2e78f041ccf54f))
* Extend logging schema to include new error states ([9aef30c](https://github.com/disafronov/etl-prometheus2clickhouse/commit/9aef30cde157edc2a73e73a710558a5d4c5b2f5a))
* Implement HTTP streaming for file insertion in ClickHouseClient ([2fd052d](https://github.com/disafronov/etl-prometheus2clickhouse/commit/2fd052dcf105f28f3454d5d901ff3e61db596c97))
* Implement state management in ClickHouseClient and ETL job ([38240f9](https://github.com/disafronov/etl-prometheus2clickhouse/commit/38240f99676fbea0df84d6d6c5c004309844d588))
* Optimize file insertion handling in ClickHouseClient ([bc7df92](https://github.com/disafronov/etl-prometheus2clickhouse/commit/bc7df92f7e9a62c783073088f1480daa334a4774))
* Update logging schema for ETL job error handling ([4a296a9](https://github.com/disafronov/etl-prometheus2clickhouse/commit/4a296a9ea1019fae66ba7b991d6683d53c175c2b))
* Update logging schema to version 3 and change data types for window start and end fields ([0d4d9fb](https://github.com/disafronov/etl-prometheus2clickhouse/commit/0d4d9fb0d4087ea47cc5fc448f1b9ceda5fcc13b))

### Bug Fixes

* Improve error handling and logging in ClickHouseClient's file insertion ([7744ab6](https://github.com/disafronov/etl-prometheus2clickhouse/commit/7744ab6d3df95461db918ee02e5cda8cc83591ef))
* Improve error logging in ClickHouseClient and EtlJob ([6a3aa51](https://github.com/disafronov/etl-prometheus2clickhouse/commit/6a3aa5137be354b1fdcf451a63062ba8683a939a))

## [1.1.0-rc.2](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.1.0-rc.1...v1.1.0-rc.2) (2025-12-25)

### Features

* Add insert_from_file method to ClickHouseClient for efficient data insertion ([be9e1b3](https://github.com/disafronov/etl-prometheus2clickhouse/commit/be9e1b3e2951e78095a073cbbc06858620112fc7))
* Enhance ETL job and ClickHouseClient with improved file handling and logging ([a76ec36](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a76ec369beb21b1ddfe638a870ab60ae56a9790f))

## [1.1.0-rc.1](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.0.0...v1.1.0-rc.1) (2025-12-25)

### Features

* Enhance error handling and logging in PrometheusClient ([0563fb5](https://github.com/disafronov/etl-prometheus2clickhouse/commit/0563fb5791725716202b5cccb620571a99a76a6b))

## 1.0.0 (2025-12-25)

### Features

* add Docker Compose configuration for ClickHouse, Prometheus, and PushGateway ([cc572ef](https://github.com/disafronov/etl-prometheus2clickhouse/commit/cc572ef9654b5c424ad5b042b03fcbf0a24c8a60))
* add TLS verification control for ClickHouse and PushGateway clients ([3ad724e](https://github.com/disafronov/etl-prometheus2clickhouse/commit/3ad724e9b69479e9b2869e2e1150b14ce12f5413))
* enhance error handling for invalid value pairs in ETL job ([1f2696c](https://github.com/disafronov/etl-prometheus2clickhouse/commit/1f2696ce09e7d264f89268770333f1fed9606d77))
* enhance logging for ETL job data fetching ([85ee163](https://github.com/disafronov/etl-prometheus2clickhouse/commit/85ee16361dfda7138f05ead73c979a44ce789e9a))
* enhance logging throughout ETL job process ([2e63d5a](https://github.com/disafronov/etl-prometheus2clickhouse/commit/2e63d5ad1083e65eaabf9a4bbfb5096f24dca468))
* enhance password normalization in PushGateway configuration ([9b50c29](https://github.com/disafronov/etl-prometheus2clickhouse/commit/9b50c29bda87ee7834ab3e17eea345f90007350c))
* initial ([96c9556](https://github.com/disafronov/etl-prometheus2clickhouse/commit/96c95564fec011a2d17978952426b3586f800b38))
* introduce batch window overlap configuration for ETL job ([1bb4744](https://github.com/disafronov/etl-prometheus2clickhouse/commit/1bb47442527c8d91a748a645cf6761b47facf1ac))
* normalize ClickHouse password handling in configuration ([9f9e6c2](https://github.com/disafronov/etl-prometheus2clickhouse/commit/9f9e6c2a0cee2abadc3ed4147a0d46087066c812))
* normalize password handling in Prometheus configuration ([9b25946](https://github.com/disafronov/etl-prometheus2clickhouse/commit/9b259462dca4fd0ae0cc9519c854ba40c5549e6d))

### Bug Fixes

* ClickHouseClient URL handling ([a4e520c](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a4e520c9aba713600f632373fb7c30f27711000f))
* correct variable reference in ETL progress logging within Docker Compose ([4e5af16](https://github.com/disafronov/etl-prometheus2clickhouse/commit/4e5af1697b33cdb18c99866d1c57583f2d549256))
* ensure timestamp metrics are integers in PushGatewayClient ([271572c](https://github.com/disafronov/etl-prometheus2clickhouse/commit/271572c7b7af4defa153dde3567a9de815d4cc10))
* improve progress calculation and logging in ETL job ([7234914](https://github.com/disafronov/etl-prometheus2clickhouse/commit/72349144eb2c63b787a53b1414b492999fff8a19))
* prevent future timestamp errors in ETL job progress calculation ([79bc151](https://github.com/disafronov/etl-prometheus2clickhouse/commit/79bc151bd461ad4f203da45a1940eee531d481d2))
* update logging level for job status checks in ETL job ([62d2590](https://github.com/disafronov/etl-prometheus2clickhouse/commit/62d2590e6be280ef93bcef6211d119469eb7dbba))

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
