## [5.1.0-rc.2](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v5.1.0-rc.1...v5.1.0-rc.2) (2026-01-01)

### Features

* Add new logging fields for job running status in ClickHouse schema ([02c6363](https://github.com/disafronov/etl-prometheus2clickhouse/commit/02c636382e468789b1366a480b00511ff84d7d1c))

## [5.1.0-rc.1](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v5.0.0...v5.1.0-rc.1) (2026-01-01)

### Features

* Add batch_skipped_count to ClickHouse ETL state management ([fc7fa44](https://github.com/disafronov/etl-prometheus2clickhouse/commit/fc7fa440c9780b2fef57073066b4fbc8f98ff74a))
* Enhance logging for unparseable Prometheus values in ETL job ([6469f86](https://github.com/disafronov/etl-prometheus2clickhouse/commit/6469f8673fb98a7016c9f2a11a5baedbfaee41b8))

## [5.0.0](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v4.2.0...v5.0.0) (2026-01-01)

### ⚠ BREAKING CHANGES

* labels format

Signed-off-by: Dmitrii Safronov <zimniy@cyberbrain.cc>

### Features

* Add logging for transformed file details in ETL job ([19e5225](https://github.com/disafronov/etl-prometheus2clickhouse/commit/19e5225a4815c55e2d5e65864d0ad888162cd9e9))
* Add minimum window start timestamp configuration and clamping logic ([9ffeb36](https://github.com/disafronov/etl-prometheus2clickhouse/commit/9ffeb3665d358bb3cfe9f4bb087e85f1e426be80))
* Add Unix timestamp conversion for ClickHouse DateTime values ([ce6cdc5](https://github.com/disafronov/etl-prometheus2clickhouse/commit/ce6cdc5b4b2f31e77542218e20b441746ffa3cea))
* Enhance ETL job data fetching and logging ([7c77e89](https://github.com/disafronov/etl-prometheus2clickhouse/commit/7c77e89d6455c2828bfa0a13b0deac4c538e4b24))
* Enhance ETL job logging with additional output details ([a91bda2](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a91bda28abc310ffdd201c440bf11ec4afb14559))
* Enhance ETL job logging with Prometheus response details ([43a0edb](https://github.com/disafronov/etl-prometheus2clickhouse/commit/43a0edbf7be0e3fdc87b74b9848d2ce32bc118c3))
* Implement event-based streaming parsing for Prometheus responses ([a99559d](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a99559d5abf624cfd94bbe61477ab8b8178638d6))
* Implement float formatting to prevent scientific notation in ETL job ([f20132e](https://github.com/disafronov/etl-prometheus2clickhouse/commit/f20132e138b84215d24cca7efa4bb844aca12a7a))

### Bug Fixes

* Convert Unix timestamps to datetime objects in ClickHouseClient ([17a63c9](https://github.com/disafronov/etl-prometheus2clickhouse/commit/17a63c9f6910631e3020c3b6c6402bca407642c5))
* Enhance JSON parsing in ETL job with type assertions and decimal handling ([3d5fe24](https://github.com/disafronov/etl-prometheus2clickhouse/commit/3d5fe24c3291bff0c10f9043da9b45dd289194c7))
* Normalize password handling in PrometheusClient authentication ([2f6a73f](https://github.com/disafronov/etl-prometheus2clickhouse/commit/2f6a73f1ffbc5bb8d9ad53b7a30d4d78b89f6cd4))

### Code Refactoring

* Update labels structure in ClickHouse schema and ETL job ([18b614b](https://github.com/disafronov/etl-prometheus2clickhouse/commit/18b614b2054431bd2ed5461c5b7885da8b18eb1d))

## [5.0.0-rc.9](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v5.0.0-rc.8...v5.0.0-rc.9) (2026-01-01)

## [5.0.0-rc.8](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v5.0.0-rc.7...v5.0.0-rc.8) (2026-01-01)

### Bug Fixes

* Enhance JSON parsing in ETL job with type assertions and decimal handling ([6ee84ed](https://github.com/disafronov/etl-prometheus2clickhouse/commit/6ee84ed8000efbc011fa56b9ac1ab1487e51711a))

## [5.0.0-rc.7](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v5.0.0-rc.6...v5.0.0-rc.7) (2026-01-01)

### Bug Fixes

* Normalize password handling in PrometheusClient authentication ([ccca438](https://github.com/disafronov/etl-prometheus2clickhouse/commit/ccca4382c7a40e155b4c57095f3da1ff2cd2db19))

## [5.0.0-rc.6](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v5.0.0-rc.5...v5.0.0-rc.6) (2026-01-01)

### Features

* Add minimum window start timestamp configuration and clamping logic ([7fd1a37](https://github.com/disafronov/etl-prometheus2clickhouse/commit/7fd1a37682352400b3399898d880a85dc3e0e88c))

## [5.0.0-rc.5](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v5.0.0-rc.4...v5.0.0-rc.5) (2026-01-01)

### Features

* Implement event-based streaming parsing for Prometheus responses ([2e83e5c](https://github.com/disafronov/etl-prometheus2clickhouse/commit/2e83e5cbf2e619364519c32536164c84a7e13fb7))
* Implement float formatting to prevent scientific notation in ETL job ([786d560](https://github.com/disafronov/etl-prometheus2clickhouse/commit/786d560a0b03d0ae77134839223d9f165b7e9227))

## [5.0.0-rc.4](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v5.0.0-rc.3...v5.0.0-rc.4) (2026-01-01)

### Features

* Add logging for transformed file details in ETL job ([d9be8d0](https://github.com/disafronov/etl-prometheus2clickhouse/commit/d9be8d0bae9dae8bc5b30938c0ad393bd8b4094a))
* Enhance ETL job data fetching and logging ([96e93f1](https://github.com/disafronov/etl-prometheus2clickhouse/commit/96e93f1fb29b4b4115f6a40fcfd02b28f9c5a32d))
* Enhance ETL job logging with additional output details ([799b759](https://github.com/disafronov/etl-prometheus2clickhouse/commit/799b75914722174db8a07ce8f95e2b3645bd805c))
* Enhance ETL job logging with Prometheus response details ([91ff393](https://github.com/disafronov/etl-prometheus2clickhouse/commit/91ff393017f31495d296d307eb70b45c9fc6c11c))

## [5.0.0-rc.3](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v5.0.0-rc.2...v5.0.0-rc.3) (2026-01-01)

### Bug Fixes

* Convert Unix timestamps to datetime objects in ClickHouseClient ([a347651](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a34765170b6791702788711a0d5ae5e1578aaec3))

## [5.0.0-rc.2](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v5.0.0-rc.1...v5.0.0-rc.2) (2025-12-30)

### Features

* Add Unix timestamp conversion for ClickHouse DateTime values ([5b10ccc](https://github.com/disafronov/etl-prometheus2clickhouse/commit/5b10ccc437472d54ebd1c90074024d940fcfef66))

## [4.2.1-rc.1](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v4.2.0...v4.2.1-rc.1) (2025-12-29)

### ⚠ BREAKING CHANGES

* labels format

Signed-off-by: Dmitrii Safronov <zimniy@cyberbrain.cc>

### Code Refactoring

* Update labels structure in ClickHouse schema and ETL job ([5e3e487](https://github.com/disafronov/etl-prometheus2clickhouse/commit/5e3e4873aed30b528249505ba8fa203a29c9bb36))

## [4.2.0](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v4.1.0...v4.2.0) (2025-12-28)

### Features

* **logging:** Enhance logging configuration with ECS formatter ([14a69a8](https://github.com/disafronov/etl-prometheus2clickhouse/commit/14a69a86f5b307823c01c9665e313366fb973dac))

## [4.2.0-rc.1](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v4.1.1...v4.2.0-rc.1) (2025-12-28)

### Features

* **logging:** Enhance logging configuration with ECS formatter ([bb57d4f](https://github.com/disafronov/etl-prometheus2clickhouse/commit/bb57d4f49bc667909a36a758b07456b32dfbb115))

## [4.1.0](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v4.0.0...v4.1.0) (2025-12-26)

### Features

* Add project information retrieval from pyproject.toml ([a54e572](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a54e5722b6bb62af362f1cbf6ad847f038e1f22d))

### Bug Fixes

* Clarify actual window calculation in ETL job ([02a97e1](https://github.com/disafronov/etl-prometheus2clickhouse/commit/02a97e1bef7835913c6ee9c989dae91a1fe18be1))
* Extend logging schema with new field for Prometheus response path ([60a94bb](https://github.com/disafronov/etl-prometheus2clickhouse/commit/60a94bb0e2008f55cb196965f8f0ae8be296a747))
* Improve logging for progress adjustment in ETL job ([430cce6](https://github.com/disafronov/etl-prometheus2clickhouse/commit/430cce651d861768346425099c27d279a8d9c0a5))

## [4.1.0-rc.3](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v4.1.0-rc.2...v4.1.0-rc.3) (2025-12-26)

### Bug Fixes

* Clarify actual window calculation in ETL job ([0cc939f](https://github.com/disafronov/etl-prometheus2clickhouse/commit/0cc939f80e039271f67c958a30732cbe9c6512b2))

## [4.1.0-rc.2](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v4.1.0-rc.1...v4.1.0-rc.2) (2025-12-26)

### Bug Fixes

* Improve logging for progress adjustment in ETL job ([95d5511](https://github.com/disafronov/etl-prometheus2clickhouse/commit/95d5511ff18da2d5ee24d6b6801e0e5300cf6acf))

## [4.1.0-rc.1](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v4.0.1-rc.1...v4.1.0-rc.1) (2025-12-26)

### Features

* Add project information retrieval from pyproject.toml ([d3cbca0](https://github.com/disafronov/etl-prometheus2clickhouse/commit/d3cbca0a7d72e189f0c48412e1389d24279af747))

## [4.0.1-rc.1](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v4.0.0...v4.0.1-rc.1) (2025-12-26)

### Bug Fixes

* Extend logging schema with new field for Prometheus response path ([d26fdfc](https://github.com/disafronov/etl-prometheus2clickhouse/commit/d26fdfc948ee4b6c13cc230c9e5f962e98d06a84))

## [3.1.0](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.0.1...v3.1.0) (2025-12-26)

### ⚠ BREAKING CHANGES

* Updated all instances of 'metric_name' to 'name' in the ClickHouse client, ETL job, and related tests to maintain consistency in naming conventions.

Signed-off-by: Dmitrii Safronov <zimniy@cyberbrain.cc>

### Features

* Add error handling for running job failures in logging schema ([81b2020](https://github.com/disafronov/etl-prometheus2clickhouse/commit/81b2020800a31e055126ca474c770a1c8ac3f323))
* Enhance job management in ClickHouseClient with running job timestamp retrieval ([6837f07](https://github.com/disafronov/etl-prometheus2clickhouse/commit/6837f07c1cf534473356905f65ea438de58d4d0f))
* Extend logging schema with new error handling fields ([52bf5ff](https://github.com/disafronov/etl-prometheus2clickhouse/commit/52bf5ff1d7b37aa52ae6457ceae56c56ec2cf7e9))
* Implement streaming ETL pipeline in ETL job ([0aa1b9a](https://github.com/disafronov/etl-prometheus2clickhouse/commit/0aa1b9a9bfd9289d65f2ad6b0840166db78de28d))

### Code Refactoring

* Rename metric_name to name across the codebase ([707dc2b](https://github.com/disafronov/etl-prometheus2clickhouse/commit/707dc2b3bdce515778b9a34dce6062967e7c75e1))

# Changelog

<!-- markdownlint-disable MD024 -->

## [4.0.0-rc.3](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v4.0.0-rc.2...v4.0.0-rc.3) (2025-12-26)

## [4.0.0-rc.2](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v4.0.0-rc.1...v4.0.0-rc.2) (2025-12-26)

### Features

* Implement streaming ETL pipeline in ETL job ([916d901](https://github.com/disafronov/etl-prometheus2clickhouse/commit/916d901864a51363fae0b3c2826ec6ee97421e6d))

## [4.0.0-rc.1](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.1.0-rc.5...v4.0.0-rc.1) (2025-12-26)

### ⚠ BREAKING CHANGES

* Updated all instances of 'metric_name' to 'name' in the ClickHouse client, ETL job, and related tests to maintain consistency in naming conventions.

Signed-off-by: Dmitrii Safronov <zimniy@cyberbrain.cc>

### Code Refactoring

* Rename metric_name to name across the codebase ([39f1d7e](https://github.com/disafronov/etl-prometheus2clickhouse/commit/39f1d7ef969734d978fd740ec26d7d8ba046e378))

## [3.1.0-rc.5](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.1.0-rc.4...v3.1.0-rc.5) (2025-12-26)

## [3.1.0-rc.4](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.1.0-rc.3...v3.1.0-rc.4) (2025-12-26)

### Features

* Extend logging schema with new error handling fields ([ebc0712](https://github.com/disafronov/etl-prometheus2clickhouse/commit/ebc0712ea0e955ce1d83124d0bf123a8c25be9f4))

## [3.1.0-rc.3](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.1.0-rc.2...v3.1.0-rc.3) (2025-12-26)

## [3.1.0-rc.2](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.1.0-rc.1...v3.1.0-rc.2) (2025-12-26)

### Features

* Add error handling for running job failures in logging schema ([5784b78](https://github.com/disafronov/etl-prometheus2clickhouse/commit/5784b789d52b8f1203fcecf8ce2f101bc20c1f4c))

## [3.1.0-rc.1](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.0.1...v3.1.0-rc.1) (2025-12-26)

### Features

* Enhance job management in ClickHouseClient with running job timestamp retrieval ([ee9bad1](https://github.com/disafronov/etl-prometheus2clickhouse/commit/ee9bad1a6a55ad4aa233b95699ef8323b9eb64bd))

## [3.0.1](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.0.0...v3.0.1) (2025-12-26)

## [3.0.1-rc.1](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.0.0...v3.0.1-rc.1) (2025-12-26)

## [1.1.0](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v1.0.0...v1.1.0) (2025-12-26)

### ⚠ BREAKING CHANGES

* new columns order

Signed-off-by: Dmitrii Safronov <zimniy@cyberbrain.cc>

* Update logging schema to version 3 and change data types for window start and end fields
* Removed PushGatewayClient and related configurations, streamlining the ETL process. Timestamp parameters now use int instead of float.

Signed-off-by: Dmitrii Safronov <zimniy@cyberbrain.cc>

### Features

* Add insert_from_file method to ClickHouseClient for efficient data insertion ([7952af4](https://github.com/disafronov/etl-prometheus2clickhouse/commit/7952af4100d37421b1c2329eece37b0c8ed86c0e))
* Add integer validation for SQL insertion in ClickHouseClient ([d1706ea](https://github.com/disafronov/etl-prometheus2clickhouse/commit/d1706ea8e742259de425998d05e6506f8a23ffb1))
* Add job management methods to ClickHouseClient ([5307c7a](https://github.com/disafronov/etl-prometheus2clickhouse/commit/5307c7a88cd8c8026d4ee14793d72b662a8551c4))
* Add table name validation in ClickHouseClient ([ea7067d](https://github.com/disafronov/etl-prometheus2clickhouse/commit/ea7067d54036477d7294c8c8602ed11903c34286))
* Add validation for batch window size and overlap in EtlConfig ([a0afb97](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a0afb97ee05d3256b025a91d59771dd70a96f353))
* Add warning for large files in insert_from_file() fallback ([bca936a](https://github.com/disafronov/etl-prometheus2clickhouse/commit/bca936a734fbdfb45725a2f344dfebfec446080f))
* enhance ClickHouse table definitions with ZSTD compression ([a801a16](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a801a16adcd898240866666df8fe90ed8bfc005b))
* Enhance error handling and logging in PrometheusClient ([efe1174](https://github.com/disafronov/etl-prometheus2clickhouse/commit/efe1174da21de049bdd412604259781f80b9b9dc))
* Enhance error handling in EtlJob for job start conditions ([f4c30e6](https://github.com/disafronov/etl-prometheus2clickhouse/commit/f4c30e6200f52c3382d1b9086c68aa774cef64b4))
* Enhance ETL job and ClickHouseClient with improved file handling and logging ([51524f7](https://github.com/disafronov/etl-prometheus2clickhouse/commit/51524f7ab0580bff643ea48ea08833fc434b823a))
* Enhance logging with formatted UTC timestamps ([9cccfbc](https://github.com/disafronov/etl-prometheus2clickhouse/commit/9cccfbce56bfe503e2568fd3251cd6cd6caf478e))
* Extend logging schema to include new error states ([6d32431](https://github.com/disafronov/etl-prometheus2clickhouse/commit/6d32431e4b0d727bbac06f7e448b17600964eb8b))
* Implement HTTP streaming for file insertion in ClickHouseClient ([9fb2783](https://github.com/disafronov/etl-prometheus2clickhouse/commit/9fb2783674ae01c7aec26194279d757ed19328b8))
* Implement state management in ClickHouseClient and ETL job ([59063ff](https://github.com/disafronov/etl-prometheus2clickhouse/commit/59063ffca060b410783abc9a0544bb120c07562a))
* Optimize file insertion handling in ClickHouseClient ([109a587](https://github.com/disafronov/etl-prometheus2clickhouse/commit/109a58793ebe9cf4730b704182bc29dbcefd15bc))
* Update logging schema for ETL job error handling ([3a8beb8](https://github.com/disafronov/etl-prometheus2clickhouse/commit/3a8beb885b6bef9aeae01ed7ba380cb91e3582a0))
* Update logging schema to version 3 and change data types for window start and end fields ([4bd849b](https://github.com/disafronov/etl-prometheus2clickhouse/commit/4bd849bd92d00d2704ef3733ff380ffa4e00dd95))

### Bug Fixes

* Correct window end calculation in ETL job to maintain consistent window size ([4b4ff0b](https://github.com/disafronov/etl-prometheus2clickhouse/commit/4b4ff0b483c802a09a47b2eea8974a48596d6aab))
* Enhance error logging in ClickHouseClient and main application ([90c0f98](https://github.com/disafronov/etl-prometheus2clickhouse/commit/90c0f98d3a148273e41b832ab819eb2c3c5df77e))
* Improve error handling and logging in ClickHouseClient's file insertion ([fd0e455](https://github.com/disafronov/etl-prometheus2clickhouse/commit/fd0e455c2c477736a667d078a67f85465e8fefb5))
* Improve error logging in ClickHouseClient and EtlJob ([1d4cbb6](https://github.com/disafronov/etl-prometheus2clickhouse/commit/1d4cbb681c65c0662f30672af70d8cf1d6f19052))

### Code Refactoring

* Update ClickHouse ETL state handling and improve documentation ([05815b3](https://github.com/disafronov/etl-prometheus2clickhouse/commit/05815b3e95e65aa415715ad3e1a834ec53559611))

## [3.0.0-rc.5](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.0.0-rc.4...v3.0.0-rc.5) (2025-12-26)

### Bug Fixes

* Correct window end calculation in ETL job to maintain consistent window size ([53925ee](https://github.com/disafronov/etl-prometheus2clickhouse/commit/53925eeb5b9fd4305ff632295bf4fa95abae18eb))

## [3.0.0-rc.4](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.0.0-rc.3...v3.0.0-rc.4) (2025-12-26)

### Features

* Add job management methods to ClickHouseClient ([a1e56b9](https://github.com/disafronov/etl-prometheus2clickhouse/commit/a1e56b9b1d4d01390e4fb332aee4030701724026))

## [3.0.0-rc.3](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.0.0-rc.2...v3.0.0-rc.3) (2025-12-25)

## [3.0.0-rc.2](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v3.0.0-rc.1...v3.0.0-rc.2) (2025-12-25)

### Features

* enhance ClickHouse table definitions with ZSTD compression ([48c149f](https://github.com/disafronov/etl-prometheus2clickhouse/commit/48c149f58fe837d5005acd80cd11440bfe0bca7e))

## [2.0.0-rc.5](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v2.0.0-rc.4...v2.0.0-rc.5) (2025-12-25)

### ⚠ BREAKING CHANGES

* new columns order

Signed-off-by: Dmitrii Safronov <zimniy@cyberbrain.cc>

### Code Refactoring

* Update ClickHouse ETL state handling and improve documentation ([c7b91bb](https://github.com/disafronov/etl-prometheus2clickhouse/commit/c7b91bb17d829b6243d32cd2a21199c441beb25b))

## [2.0.0-rc.4](https://github.com/disafronov/etl-prometheus2clickhouse/compare/v2.0.0-rc.3...v2.0.0-rc.4) (2025-12-25)

### Features

* Enhance logging with formatted UTC timestamps ([9eaaf13](https://github.com/disafronov/etl-prometheus2clickhouse/commit/9eaaf134250ba44c7133b59a612fd2214eaa22c4))

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
