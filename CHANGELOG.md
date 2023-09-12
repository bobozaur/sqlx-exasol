# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).
 
## [0.7.1-alpha-3] - 2023-09-12
 
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