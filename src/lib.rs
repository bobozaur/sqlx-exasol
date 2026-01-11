#![cfg_attr(not(test), warn(unused_crate_dependencies))]
//! A database driver for Exasol to be used with the Rust [sqlx](https://github.com/launchbadge/sqlx) framework.
//!
//! Based on `sqlx` version `0.9.0-alpha.1`.
//!
//! ## Features flags
//!
//! - `etl` - Add support for ETL jobs.
//! - `compression` - Add compression support (for both connections and ETL jobs).
//! - `any` - Add support for the `Any` database driver, which can proxy to a database driver at
//!   runtime.
//! - `derive` - Add support for the derive family macros, those are `FromRow`, `Type`, `Encode`,
//!   `Decode`.
//! - `macros` - Add support for the `query*!` macros, which allows compile-time checked queries.
//! - `migrate` - Add support for the migration management and `migrate!` macro, which allow
//!   compile-time embedded migrations.
//! - `uuid` - Add support for UUID.
//! - `chrono` - Add support for date and time types from `chrono`.
//! - `time` - Add support for date and time types from `time` crate (alternative to `chrono`, which
//!   is preferred by `query!` macro, if both enabled).
//! - `bigdecimal` - Add support for `BigDecimal` from the `bigdecimal` crate.
//! - `rust_decimal` - Add support for `Decimal` from the `rust_decimal` crate.
//! - `geo-types` - Add support for `Geometry` and its variants from the `geo-types` crate.
//! - `json` - Add support for `Json<T>` as well as `serde_json::Value` and `serde_json::RawValue`.
//!
//! ## Supported types
//!
//! See the [`types`] module.
//!
//! ## Comparison to native sqlx drivers
//!
//! The driver re-exports all `sqlx` public API and implements the exposed traits. As a result,
//! it can do all the drivers shipped with `sqlx` do, with some caveats:
//!
//! - Limitations
//!   - separate CLI utility (`sqlx-exasol` instead of `sqlx`)
//!   - compile time query macros cannot work along the ones from `sqlx` within the same crate
//!   - no locking migrations support<sup>[1](#no_locks)</sup>
//!   - no column nullability checks<sup>[2](#nullable)</sup>
//!
//! - Additions
//!   - array-like parameter binding in queries, thanks to the columnar nature of the Exasol
//!     database
//!   - performant & parallelizable ETL IMPORT/EXPORT jobs in CSV format through HTTP Transport
//!
//! ## Compile-time query checks
//!
//! The driver now supports compile-time query validation.
//!
//! However, full functionality is implemented through path overrides and due to `sqlx` macros
//! implementation details you will currently need to either add `extern crate sqlx_exasol as sqlx;`
//! to the root of your crate or rename the crate import to `sqlx` in `Cargo.toml`:
//!
//! ```toml
//! sqlx = { version = "*", package = "sqlx-exasol" }
//! ```
//!
//! This implies that the compile time query macros from both `sqlx-exasol` and `sqlx`
//! cannot co-exist within the same crate without collisions or unexpected surprises.
//!
//! See <https://github.com/launchbadge/sqlx/pull/3944> for more details.
//!
//! ## CLI utility
//!
//! The driver uses its own CLI utility (built on the same `sqlx-cli` library):
//! ```sh
//! cargo install sqlx-exasol-cli
//!
//! # Usage is exactly the same as sqlx-cli
//! sqlx-exasol database create
//! sqlx-exasol database drop
//! sqlx-exasol migrate add <name>
//! sqlx-exasol migrate run
//! cargo sqlx-exasol prepare
//! ```
//!
//! ## Connection string
//!
//! The connection string is expected to be an URL with the `exa://` scheme, e.g:
//! `exa://sys:exasol@localhost:8563`.
//!
//! See [`ExaConnectOptions`] for a list of supported connection string parameters.
//!
//! ## HTTP Transport
//! Functionality that allows performant data import/export by creation of one-shot HTTP servers
//! to which Exasol connects to (at most one per node), thus balancing the load.
//!
//! The data is always in `CSV` format and job configuration can be done through the
//! [`ImportBuilder`](etl::ImportBuilder) and [`ExportBuilder`](etl::ExportBuilder) structs.
//! The workers implement `AsyncWrite` and `AsyncRead` respectively, providing great flexibility in
//! terms of how the data is processed.
//!
//! The general flow of an ETL job is:
//! - build the job through [`ImportBuilder`](etl::ImportBuilder) or
//!   [`ExportBuilder`](etl::ExportBuilder)
//! - concurrently wait on the query execution future (typically from the main thread) and on worker
//!   operations (async tasks can be spawned in multi-threaded runtimes to further parallelize the
//!   workload).
//! - when all the workers are done (readers reach EOF, while writers require an explicit `close()`)
//!   the job ends and the query execution future returns.
//! - an error/timeout issue results in the query execution future or a worker throwing an error,
//!   therefore consider joining the tasks and aborting them if an error is thrown somewhere.
//!
//! ## Examples
//! Using the driver for regular database interactions:
//! ```rust,no_run
//! use std::env;
//!
//! use sqlx_exasol::{error::*, *};
//!
//! # async {
//! #
//! let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
//! let mut con = pool.acquire().await?;
//!
//! sqlx_exasol::query("CREATE SCHEMA RUST_DOC_TEST")
//!     .execute(&mut *con)
//!     .await?;
//! #
//! # let res: Result<(), BoxDynError> = Ok(());
//! # res
//! # };
//! ```
//!
//! Array-like parameter binding, also featuring the [`crate::types::ExaIter`] adapter.
//! An important thing to note is that the parameter sets must be of equal length,
//! otherwise an error is thrown:
//! ```rust,no_run
//! use std::{collections::HashSet, env};
//!
//! use sqlx_exasol::{error::*, *};
//!
//! # async {
//! #
//! let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
//! let mut con = pool.acquire().await?;
//!
//! let params1 = vec![1, 2, 3];
//! let params2 = HashSet::from([1, 2, 3]);
//!
//! sqlx_exasol::query("INSERT INTO MY_TABLE VALUES (?, ?)")
//!     .bind(&params1)
//!     .bind(types::ExaIter::new(params2.iter()))
//!     .execute(&mut *con)
//!     .await?;
//! #
//! # let res: Result<(), BoxDynError> = Ok(());
//! # res
//! # };
//! ```
//!
//! An EXPORT - IMPORT ETL data pipe.
//! ```rust,no_run
//! # #[cfg(feature = "etl")] {
//! use std::env;
//!
//! use futures_util::{
//!     future::{try_join, try_join3, try_join_all},
//!     AsyncReadExt, AsyncWriteExt, TryFutureExt,
//! };
//! use sqlx_exasol::{error::*, etl::*, *};
//!
//! async fn pipe(mut reader: ExaExport, mut writer: ExaImport) -> Result<(), BoxDynError> {
//!     let mut buf = vec![0; 5120].into_boxed_slice();
//!     let mut read = 1;
//!
//!     while read > 0 {
//!         // Readers return EOF when there's no more data.
//!         read = reader.read(&mut buf).await?;
//!         // Write data to Exasol
//!         writer.write_all(&buf[..read]).await?;
//!     }
//!
//!     // Writes, unlike readers, MUST be closed to signal we won't send more data to Exasol
//!     writer.close().await?;
//!     Ok(())
//! }
//!
//! # async {
//! #
//! let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
//! let mut con1 = pool.acquire().await?;
//! let mut con2 = pool.acquire().await?;
//!
//! // Build EXPORT job
//! let (export_fut, readers) = ExportBuilder::new_from_table("TEST_ETL")
//!     .build(&mut con1)
//!     .await?;
//!
//! // Build IMPORT job
//! let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut con2).await?;
//!
//! // Use readers and writers in some futures
//! let transport_futs = std::iter::zip(readers, writers).map(|(r, w)| pipe(r, w));
//!
//! // Execute the EXPORT and IMPORT query futures along with the worker futures
//! let (export_res, import_res, _) = try_join3(
//!     export_fut.map_err(From::from),
//!     import_fut.map_err(From::from),
//!     try_join_all(transport_futs),
//! )
//! .await?;
//!
//! assert_eq!(export_res.rows_affected(), import_res.rows_affected());
//! #
//! # let res: Result<(), BoxDynError> = Ok(());
//! # res
//! # }};
//! ```
//!
//! ## Footnotes
//! <a name="no_locks">1</a>: Exasol has no advisory or database locks and simple, unnested,
//! transactions are unfortunately not enough to define a mechanism so that concurrent migrations do
//! not collide. This does **not** pose a problem when migrations are run sequentially or do not act
//! on the same database objects.
//!
//! <a name="nullable">2</a>: Exasol does not provide the information of whether a column is
//! nullable or not, so the driver cannot implicitly decide whether a `NULL` value can go into a
//! certain database column or not until it actually tries.

pub use sqlx::*;
pub use sqlx_exasol_impl::*;

/// # Supported types
///
/// | Rust type                 | Exasol type                                   |
/// | :------------------------ | :-------------------------------------------- |
/// | `bool`                    | `BOOLEAN`                                     |
/// | `i8`, `i16`, `i32`, `i64` | `DECIMAL`                                     |
/// | `f64`                     | `DOUBLE`                                      |
/// | `String`, `&str`          | `CHAR(n) ASCII/UTF8`, `VARCHAR(n) ASCII/UTF8` |
/// | `ExaIntervalYearToMonth`  | `INTERVAL YEAR TO MONTH`                      |
/// | `HashType`                | `HASHTYPE`                                    |
/// | `Option<T>`               | `T` (for any `T` that implements `Type`)      |
///
/// ## `chrono` feature
///
/// | Rust type               | Exasol type              |
/// | :---------------------- | :----------------------- |
/// | `chrono::NaiveDate`     | `DATE`                   |
/// | `chrono::NaiveDateTime` | `TIMESTAMP`              |
/// | `chrono::TimeDelta`     | `INTERVAL DAY TO SECOND` |
///
/// ## `time` feature
///
/// | Rust type                 | Exasol type              |
/// | :------------------------ | :----------------------- |
/// | `time::Date`              | `DATE`                   |
/// | `time::PrimitiveDateTime` | `TIMESTAMP`              |
/// | `time::Duration`          | `INTERVAL DAY TO SECOND` |
///
/// ## `rust_decimal` feature
///
/// | Rust type               | Exasol type    |
/// | :---------------------- | :------------- |
/// | `rust_decimal::Decimal` | `DECIMAL(p,s)` |
///
/// ## `bigdecimal` feature
///
/// | Rust type                | Exasol type    |
/// | :----------------------- | :------------- |
/// | `bigdecimal::BigDecimal` | `DECIMAL(p,s)` |
///
/// ## `uuid` feature
///
/// | Rust type    | Exasol type |
/// | :----------- | :---------- |
/// | `uuid::Uuid` | `HASHTYPE`  |
///
/// ## `geo-types` feature
///
/// | Rust type             | Exasol type |
/// | :-------------------- | :---------- |
/// | `geo_types::Geometry` | `GEOMETRY`  |
///
/// **Note:** due to a [bug in the Exasol websocket
/// API](httpsf://github.com/exasol/websocket-api/issues/39), `GEOMETRY` can't be used as prepared
/// statement bind parameters. It can, however, be used as a column in a returned result set or with
/// runtime checked queries.
///
/// ## `json` feature
///
/// The `json` feature enables  `Encode` and `Decode` implementations for `Json<T>`,
/// `serde_json::Value` and `&serde_json::value::RawValue`.
///
/// ## Array-like parameters
///
/// Array-like types can be passed as parameters, including in compile time checked queries,
/// for batch parameter binding due to Exasol's columnar nature.
///
/// Supported types are [`Vec<T>`], `&T` (slices), [`[T;N]`] (arrays), iterators through the
/// [`ExaIter`](crate::types::ExaIter) adapter, etc.
///
/// Parameter arrays must be of equal length (runtime checked) or an error will be thrown otherwise.
///
/// Custom types that implement [`Type`] can be used in array-like types by implementing
/// the [`ExaHasArrayType`](crate::types::ExaHasArrayType) marker trait for them.
pub mod types {
    pub use sqlx::types::*;
    pub use sqlx_exasol_impl::types::*;

    #[cfg(feature = "chrono")]
    pub mod chrono {
        pub use sqlx::types::chrono::*;
        pub use sqlx_exasol_impl::types::chrono::*;
    }

    #[cfg(feature = "time")]
    pub mod time {
        pub use sqlx::types::time::*;
        pub use sqlx_exasol_impl::types::time::*;
    }
}

pub mod any {
    pub use sqlx::any::*;
    pub use sqlx_exasol_impl::any::DRIVER;
}

#[cfg(feature = "macros")]
pub use sqlx_exasol_macros;
#[cfg(feature = "macros")]
mod macros;

#[cfg(feature = "macros")]
#[doc(hidden)]
pub mod ty_match;
