#![cfg_attr(not(test), warn(unused_crate_dependencies))]
//! **EXASOL** database driver.

#[cfg(test)]
extern crate sqlx_a_orig as sqlx;

#[cfg(feature = "native-tls")]
use native_tls as _;
#[cfg(feature = "tls")]
use rcgen as _;
#[cfg(feature = "rustls")]
use rustls as _;

#[cfg(feature = "any")]
pub mod any;
mod arguments;
mod column;
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
#[cfg(feature = "macros")]
mod type_checking;
mod type_info;
pub mod types;
mod value;

pub use arguments::ExaArguments;
pub use column::ExaColumn;
#[cfg(feature = "etl")]
pub use connection::etl;
pub use connection::ExaConnection;
pub use database::Exasol;
pub use options::{ExaCompressionMode, ExaConnectOptions, ExaConnectOptionsBuilder, ExaSslMode};
pub use query_result::ExaQueryResult;
pub use responses::{ExaAttributes, ExaDatabaseError, SessionInfo};
pub use row::ExaRow;
use sqlx_core::{
    executor::Executor, impl_acquire, impl_column_index_for_row, impl_column_index_for_statement,
    impl_into_arguments_for_arguments,
};
pub use statement::ExaStatement;
pub use transaction::ExaTransactionManager;
#[doc(hidden)]
#[cfg(feature = "macros")]
pub use type_checking::QUERY_DRIVER;
pub use type_info::ExaTypeInfo;
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

// ###################
// ##### Aliases #####
// ###################
type SqlxError = sqlx_core::Error;
type SqlxResult<T> = sqlx_core::Result<T>;
