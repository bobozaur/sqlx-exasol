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

use sqlx_core::{
    executor::Executor, impl_acquire, impl_column_index_for_row, impl_column_index_for_statement,
    impl_into_arguments_for_arguments,
};

pub use arguments::ExaArguments;
pub use column::ExaColumn;
pub use connection::ExaConnection;
pub use database::Exasol;
pub use options::{ExaConnectOptions, ExaSslMode, ProtocolVersion};
pub use query_result::ExaQueryResult;
pub use responses::ExaDatabaseError;
pub use row::ExaRow;
pub use statement::ExaStatement;
pub use transaction::ExaTransactionManager;
pub use type_info::ExaTypeInfo;
pub use types::ExaIter;
#[cfg(feature = "chrono")]
pub use types::Months;
pub use value::{ExaValue, ExaValueRef};

/// An alias for [`Pool`][crate::pool::Pool], specialized for Exasol.
pub type ExaPool = sqlx_core::pool::Pool<Exasol>;

/// An alias for [`PoolOptions`][crate::pool::PoolOptions], specialized for Exasol.
pub type ExaPoolOptions = sqlx_core::pool::PoolOptions<Exasol>;

/// An alias for [`Executor<'_, Database = Exasol>`][Executor].
pub trait ExaExecutor<'c>: Executor<'c, Database = Exasol> {}
impl<'c, T: Executor<'c, Database = Exasol>> ExaExecutor<'c> for T {}

impl_into_arguments_for_arguments!(ExaArguments);
impl_acquire!(Exasol, ExaConnection);
impl_column_index_for_row!(ExaRow);
impl_column_index_for_statement!(ExaStatement);
