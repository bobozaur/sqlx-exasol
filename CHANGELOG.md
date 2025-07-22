# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

### Changed

- [#33](https://github.com/bobozaur/sqlx-exasol/pull/33): Avoid nested boxing in Executor impl
- [#31](https://github.com/bobozaur/sqlx-exasol/pull/31): BREAKING CHANGES
  - Renamed `ExaConnection::socket_addr` to `ExaConnection::server`
  - Made `feedback_interval` match between options and attributes
  - Removed the boxing of `EtlQuery` future
  - Made `ExaAttributes::set_autocommit` private
  - Created wrapper structs `ExaImport` and `ExaExport` to avoid exposing enum variants
  
## [0.8.6-hotfix1] - 2025-07-22

### Fixed

- [36](https://github.com/bobozaur/sqlx-exasol/issues/36) Fix chunked result set streaming

## [0.8.6] - 2025-06-03

### Added

- [#29](https://github.com/bobozaur/sqlx-exasol/pull/29):
  - Updated to sqlx `0.8.6`.
  - Added support for multi statement queries
  - Added `ProtocolVersion::V4`

### Changed

- [#29](https://github.com/bobozaur/sqlx-exasol/pull/29):
  - Connection attributes are only sent when changed
  - Future based I/O refactor
  - ETL code trait based improvements
  - Cancelling operations now does not leave the connection in an invalid state
  - Removed usage of `serde_transcode` in favor of little `unsafe`
  - Added deprecation warnings to `ExaExport` and `ExaImport` enum variants as well as `ExaAttributes::set_autocommit`.
  - Removed `HashType::size` field as it could not be reliably be used for data type compatibility checks because it returns the size of the string representation of the column, which depends on the session parameter `HASHTYPE_FORMAT`.

### Fixed

- [#6](https://github.com/bobozaur/sqlx-exasol/issues/6) Architecture changes made the issue obsolete.
- [#28](https://github.com/bobozaur/sqlx-exasol/issues/28) Support for multi statement queries added.

## [0.8.2] - 2024-09-05

### Added

- [#27](https://github.com/bobozaur/sqlx-exasol/pull/27): Update to sqlx `0.8.2`; Removed direct dependency on `flate2`.

## [0.7.4] - 2024-03-15

### Added

- [#25](https://github.com/bobozaur/sqlx-exasol/pull/25): ETL module refactor based on `hyper`.

## [0.7.3] - 2023-12-19

### Added

- [#22](https://github.com/bobozaur/sqlx-exasol/issues/22): Update to sqlx `0.7.3`;

## [0.7.2] - 2023-11-20

### Added

- [#21](https://github.com/bobozaur/sqlx-exasol/pull/21): Update to sqlx `0.7.2`; Simplified Windows and MacOS `cargo check`jobs.

### Fixed

- [#14](https://github.com/bobozaur/sqlx-exasol/issues/14): Simplify `ExaSocket` `AsyncWrite::poll_flush()` impl.
- [#19](https://github.com/bobozaur/sqlx-exasol/pull/19): README fixes.

## [0.7.1-alpha-4] - 2023-10-26

### Added

- [#18](https://github.com/bobozaur/sqlx-exasol/pull/18): `cargo check` CI jobs for Windows and MacOS.

### Fixed

- [#17](https://github.com/bobozaur/sqlx-exasol/issues/17) Fixed building the library after
  [CVE-2023-43669](https://nvd.nist.gov/vuln/detail/CVE-2023-43669) update of `tungstenite` to version `0.20.1`.

## [0.7.1-alpha-3] - 2023-09-13

### Added

- [#5](https://github.com/bobozaur/sqlx-exasol/issues/5): Multi-node CI testing by creating a two node database cluster.

### Changed

- [#16](https://github.com/bobozaur/sqlx-exasol/pull/16): Added the `CHANGELOG.md` file and `etl` module level docs
- [#10](https://github.com/bobozaur/sqlx-exasol/issues/10): Improved ExaConnectOptionsBuilder ergonomics by having its methods take `self`
- renamed `EtlWorker` trait to `EtlBufReader`.
- refactored `ExportReader` to implement and rely on `AsyncBufRead`.
- added `buffer_size()` method to `ExportBuilder` to be able to tweak the reader's buffer.

### Fixed

- ETL TLS with compression

## [0.7.1-alpha-2] - 2023-09-04

Second alpha release.

### Added

- Missing connection string documentation.

## [0.7.1-alpha-1] - 2023-09-04

First alpha release.
