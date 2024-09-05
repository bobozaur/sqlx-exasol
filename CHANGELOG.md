# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

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