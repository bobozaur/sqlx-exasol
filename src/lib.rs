#![cfg_attr(not(test), warn(unused_crate_dependencies))]
//! A database driver for Exasol to be used with the Rust [sqlx](https://github.com/launchbadge/sqlx) framework.
//!
//! ## Crate Features flags
//! * `etl` - enables the usage ETL jobs without TLS encryption.
//! * `etl_native_tls` - enables the `etl` feature and adds TLS encryption through
//!   `native-tls`<sup>[1](#etl_tls)</sup>
//! * `etl_rustls` - enables the `etl` feature and adds TLS encryption through
//!   `rustls`<sup>[1](#etl_tls)</sup>
//! * `compression` - enables compression support (for both connections and ETL jobs)
//! * `uuid` - enables support for the `uuid` crate
//! * `chrono` - enables support for the `chrono` crate types
//! * `rust_decimal` - enables support for the `rust_decimal` type
//! * `migrate` - enables the use of migrations and testing (just like in other `sqlx` drivers).
//!
//! ## Comparison to native sqlx drivers
//! Since the driver is used through `sqlx` and it implements the interfaces there, it can do all
//! the drivers shipped with `sqlx` do, with some caveats:
//! - Limitations
//!     - no compile-time query check support<sup>[2](#sqlx_limitations)</sup>
//!     - no `sqlx-cli` support<sup>[2](#sqlx_limitations)</sup>
//!     - no locking migrations support<sup>[3](#no_locks)</sup>
//!     - no column nullability checks<sup>[4](#nullable)</sup>
//!
//! - Additions
//!     - array-like parameter binding in queries, thanks to the columnar nature of the Exasol
//!       database
//!     - performant & parallelizable ETL IMPORT/EXPORT jobs in CSV format through HTTP Transport
//!       (see the [etl] module for more details)
//!
//! ## Connection string
//! The connection string is expected to be an URL with the `exa://` scheme, e.g:
//! `exa://sys:exasol@localhost:8563`.
//!
//! Connection options:
//! - `access-token`: Use an access token for login instead of credentials
//! - `refresh-token`: Use a refresh token for login instead of credentials
//! - `protocol-version`: Select a specific protocol version to use
//! - `ssl-mode`: Select a specifc SSL behavior. See: [`ExaSslMode`]
//! - `ssl-ca`: Use a certain certificate authority
//! - `ssl-cert`: Use a certain certificate
//! - `ssl-key`: Use a specific SSL key
//! - `statement-cache-capacity`: Set the capacity of the LRU prepared statements cache
//! - `fetch-size`: Sets the size of data chunks when retrieving result sets
//! - `query-timeout`: The query timeout amount, in seconds. 0 means no timeout
//! - `compression`: Boolean representing whether use compression
//! - `feedback-interval`: Interval at which Exasol sends keep-alive Pong frames
//!
//! [`ExaConnectOptions`] can also be constructed in code through its builder method,
//! which returns a [`ExaConnectOptionsBuilder`].
//!
//! ## Supported Rust datatypes
//! - [`bool`]
//! - [`u8`], [`u16`], [`u32`], [`u64`], [`u128`]
//! - [`i8`], [`i16`], [`i32`], [`i64`], [`i128`]
//! - [`f32`], [`f64`]
//! - [`str`], [`String`], [`std::borrow::Cow<str>`]
//! - `chrono` feature: [`chrono::DateTime<Utc>`], [`chrono::DateTime<Utc>`],
//!   [`chrono::NaiveDateTime`], [`chrono::NaiveDate`], [`chrono::Duration`], [`Months`] (analog of
//!   [`chrono::Months`])
//! - `uuid` feature: [`uuid::Uuid`]
//! - `rust_decimal` feature: [`rust_decimal::Decimal`]
//!
//! ## Supported Exasol datatypes:
//! All Exasol datatypes are supported in some way, also depdending on the additional types used
//! through feature flags.
//!
//! The [GEOMETRY](https://docs.exasol.com/db/latest/sql_references/geospatialdata/geospatialdata_overview.htm) type does not have
//! a correspondent Rust datatype. One could be introduced in future versions of the driver, but for
//! now they can be encoded/decoded to [`String`].
//!
//! ## HTTP Transport
//! Functionality that allows performant data import/export by creation of one-shot HTTP servers
//! to which Exasol connects to (at most one per node), thus balancing the load.
//!
//! The data is always in `CSV` format and job configuration can be done through the
//! [`etl::ImportBuilder`] and [`etl::ExportBuilder`] structs. The workers implement
//! [`futures_io::AsyncWrite`] and [`futures_io::AsyncRead`] respectively, providing great
//! flexibility in terms of how the data is processed.
//!
//! The general flow of an ETL job is:
//! - build the job through [`etl::ImportBuilder`] or [`etl::ExportBuilder`]
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
//! use sqlx_exasol::*;
//!
//! # async {
//! #
//! let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
//! let mut con = pool.acquire().await?;
//!
//! sqlx::query("CREATE SCHEMA RUST_DOC_TEST")
//!     .execute(&mut *con)
//!     .await?;
//! #
//! # let res: anyhow::Result<()> = Ok(());
//! # res
//! # };
//! ```
//!
//! Array-like parameter binding, also featuring the [`ExaIter`] adapter.
//! An important thing to note is that the parameter sets must be of equal length,
//! otherwise an error is thrown:
//! ```rust,no_run
//! use std::{collections::HashSet, env};
//!
//! use sqlx_exasol::*;
//!
//! # async {
//! #
//! let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
//! let mut con = pool.acquire().await?;
//!
//! let params1 = vec![1, 2, 3];
//! let params2 = HashSet::from([1, 2, 3]);
//!
//! sqlx::query("INSERT INTO MY_TABLE VALUES (?, ?)")
//!     .bind(&params1)
//!     .bind(ExaIter::from(&params2))
//!     .execute(&mut *con)
//!     .await?;
//! #
//! # let res: anyhow::Result<()> = Ok(());
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
//! use sqlx_exasol::{etl::*, *};
//!
//! async fn pipe(mut reader: ExaExport, mut writer: ExaImport) -> anyhow::Result<()> {
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
//! let (export_fut, readers) = ExportBuilder::new(ExportSource::Table("TEST_ETL"))
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
//! # let res: anyhow::Result<()> = Ok(());
//! # res
//! # }};
//! ```
//!
//! ## Footnotes
//! <a name= etl_tls>1</a>: There is unfortunately no way to automagically choose a crate's feature
//! flags based on its dependencies feature flags, so the TLS backend has to be manually selected.
//! While nothing prevents you from using, say `native-tls` with `sqlx` and `rustls` with Exasol ETL
//! jobs, it might be best to avoid compiling two different TLS backends. Therefore, consider
//! choosing the `sqlx` and `sqlx-exasol` feature flags in a consistent manner.
//!
//! <a name="sqlx_limitations">2</a>: The `sqlx` API powering the compile-time query checks and the
//! `sqlx-cli` tool is not public. Even if it were, the drivers that are incorporated into `sqlx`
//! are hardcoded in the part of the code that handles the compile-time driver decision logic.
//! <br>The main problem from what I can gather is that there's no easy way of defining a plugin
//! system in Rust at the moment, hence the hardcoding.
//!
//! <a name="no_locks">3</a>: Exasol has no advisory or database locks and simple, unnested,
//! transactions are unfortunately not enough to define a mechanism so that concurrent migrations do
//! not collide. This does **not** pose a problem when migrations are run sequentially or do not act
//! on the same database objects.
//!
//! <a name="nullable">4</a>: Exasol does not provide the information of whether a column is
//! nullable or not, so the driver cannot implicitly decide whether a `NULL` value can go into a
//! certain database column or not until it actually tries.

pub use sqlx::*;
pub use sqlx_exasol_impl::*;

pub mod types {
    pub use sqlx::types::*;
    pub use sqlx_exasol_impl::types::ExaIter;

    #[cfg(feature = "chrono")]
    pub mod chrono {
        pub use sqlx::types::chrono::*;
        pub use sqlx_exasol_impl::types::{Months, Duration};
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
