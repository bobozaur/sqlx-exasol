//! A database driver for Exasol to be used with the Rust [sqlx](https://github.com/launchbadge/sqlx) framework.  
//! **MSRV**: `1.70`
//!
//! ## Crate Features flags
//! * `etl` - enables the usage ETL jobs without TLS encryption.
//! * `etl_native_tls` - enables the `etl` feature and adds TLS encryption through `native-tls`
//! * `etl_rustls` - enables the `etl` feature and adds TLS encryption through `rustls`
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
//!     - no compile-time query check support<sup>[1](#sqlx_limitations)</sup>
//!     - no `sqlx-cli` support<sup>[1](#sqlx_limitations)</sup>
//!     - no locking migrations support<sup>[2](#no_locks)</sup>
//!     - no column nullability checks<sup>[3](#nullable)</sup>
//!     - apart from migrations, only a single query per statement is allowed (including in
//!       fixtures)<sup>[4](#single_query)</sup>
//!
//! - Additions
//!     - array-like parameter binding in queries, thanks to the columnar nature of the Exasol
//!       database
//!     - performant & parallelizable ETL IMPORT/EXPORT jobs in CSV format through HTTP Transport
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
//! An EXPORT - IMPORT ETL data pipe. The data is always in `CSV` format and some configuration can
//! be done through the [`etl::ImportBuilder`] and [`etl::ExportBuilder`] structs:
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
//! /// This is highly inefficient, as we buffer all the data in memory.
//! /// A much better way would be intermitted read and writes in a smaller, well defined buffer.
//! async fn pipe(mut reader: ExaExport, mut writer: ExaImport) -> AnyResult<()> {
//!     let mut buf = String::new();
//!     // Readers return EOF when there's no more data.
//!     reader.read_to_string(&mut buf).await?;
//!
//!     // Write data to Exasol
//!     writer.write_all(buf.as_bytes()).await?;
//!
//!     // Writes, however, MUST be closed to signal we won't send more data to Exasol
//!     writer.close().await?;
//!
//!     Ok(())
//! }
//!
//! # async {
//! #
//! let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
//! let mut con = pool.acquire().await?;
//!
//! // Build EXPORT job
//! let (export_fut, readers) = ExportBuilder::new(ExportSource::Table("TEST_ETL"))
//!     .build(&mut conn)
//!     .await?;
//!
//! // Build IMPORT job
//! let (import_fut, writers) = ImportBuilder::new("TEST_ETL").build(&mut conn).await?;
//!
//! // Use readers and writers in some futures
//! let transport_futs = iter::zip(readers, writers).map(|(r, w)| pipe(r, w));
//!
//! // Execute the EXPORT and IMPORT query futures along with the worker futures
//! let (export_res, import_res, _) = try_join3(
//!     export_fut.map_err(From::from),
//!     import_fut.map_err(From::from),
//!     try_join_all(transport_futs),
//! )
//! .await
//! .map_err(|e| anyhow::anyhow! {e})?;
//!
//! assert_eq!(export_res.rows_affected(), import_res.rows_affected());
//! #
//! # let res: anyhow::Result<()> = Ok(());
//! # res
//! # }};
//! ```
//!
//! ## Footnotes
//! <a name="sqlx_limitations">1</a>: The `sqlx` API powering the compile-time query checks and the
//! `sqlx-cli` tool is not public. Even if it were, the drivers that are incorporated into `sqlx`
//! are hardcoded in the part of the code that handles the compile-time driver decision logic.
//! <br>The main problem from what I can gather is that there's no easy way of defining a plugin
//! system in Rust at the moment, hence the hardcoding.
//!
//! <a name="no_locks">2</a>: Exasol has no advisory or database locks and simple, unnested,
//! transactions are unfortunately not enough to define a mechanism so that concurrent migrations do
//! not collide. This does **not** pose a problem when migrations are run sequentially or do not act
//! on the same database objects.
//!
//! <a name="nullable">3</a>: Exasol does not provide the information of whether a column is
//! nullable or not, so the driver cannot implicitly decide whether a `NULL` value can go into a
//! certain database column or not until it actually tries.
//!
//! <a name="single_query">4</a>: I didn't even know this (as I never even thought of doing it),
//! but `sqlx` allows running multiple queries in a single statement. Due to limitations with the
//! websocket API this driver is based on, `sqlx-exasol` can only run one query at a time. <br>This
//! is only circumvented in migrations through a somewhat limited, convoluted and possibly costly
//! workaround that tries to split queries by `;`, which does not make it applicable for runtime
//! queries at all.<br>
#![warn(
    clippy::all,
    clippy::pedantic,
    meta_variable_misuse,
    missing_abi,
    missing_copy_implementations,
    missing_debug_implementations,
    non_ascii_idents,
    pointer_structural_match,
    rust_2018_idioms,
    rust_2021_compatibility,
    single_use_lifetimes,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_lifetimes,
    unused_qualifications
)]
#![allow(single_use_lifetimes)]
#![allow(let_underscore_drop)]
#![allow(clippy::struct_excessive_bools)]
#![allow(clippy::trivially_copy_pass_by_ref)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::match_bool)]
#![allow(clippy::no_effect_underscore_binding)]
#![allow(clippy::module_name_repetitions)]

mod arguments;
mod column;
mod command;
mod connection;
mod database;
mod error;
#[cfg(feature = "migrate")]
mod migrate;
mod options;
mod query_result;
mod responses;
mod row;
mod statement;
#[cfg(feature = "migrate")]
mod testing;
mod transaction;
mod type_info;
mod types;
mod value;

pub use arguments::ExaArguments;
pub use column::ExaColumn;
#[cfg(feature = "etl")]
pub use connection::etl;
pub use connection::ExaConnection;
pub use database::Exasol;
pub use options::{ExaConnectOptions, ExaConnectOptionsBuilder, ExaSslMode, ProtocolVersion};
pub use query_result::ExaQueryResult;
pub use responses::{ExaAttributes, ExaDatabaseError, SessionInfo};
pub use row::ExaRow;
use sqlx_core::{
    executor::Executor, impl_acquire, impl_column_index_for_row, impl_column_index_for_statement,
    impl_into_arguments_for_arguments,
};
pub use statement::ExaStatement;
pub use transaction::ExaTransactionManager;
pub use type_info::ExaTypeInfo;
pub use types::ExaIter;
#[cfg(feature = "chrono")]
pub use types::Months;
pub use value::{ExaValue, ExaValueRef};

/// An alias for [`Pool`][sqlx_core::pool::Pool], specialized for Exasol.
pub type ExaPool = sqlx_core::pool::Pool<Exasol>;

/// An alias for [`PoolOptions`][sqlx_core::pool::PoolOptions], specialized for Exasol.
pub type ExaPoolOptions = sqlx_core::pool::PoolOptions<Exasol>;

/// An alias for [`Executor<'_, Database = Exasol>`][Executor].
pub trait ExaExecutor<'c>: Executor<'c, Database = Exasol> {}
impl<'c, T: Executor<'c, Database = Exasol>> ExaExecutor<'c> for T {}

impl_into_arguments_for_arguments!(ExaArguments);
impl_acquire!(Exasol, ExaConnection);
impl_column_index_for_row!(ExaRow);
impl_column_index_for_statement!(ExaStatement);
