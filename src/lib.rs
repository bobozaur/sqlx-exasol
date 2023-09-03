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
pub use responses::ExaDatabaseError;
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
