mod arguments;
mod column;
mod command;
mod connection;
pub mod database;
pub mod error;
mod options;
mod query_result;
mod responses;
mod row;
mod statement;
mod stream;
#[cfg(feature = "migrate")]
mod testing;
#[cfg(feature = "migrate")]
mod migrate;
mod tls;
mod transaction;
mod type_info;
mod types;
mod value;
mod websocket;

use arguments::ExaArguments;
use connection::ExaConnection;
use database::Exasol;
use row::ExaRow;
use sqlx_core::{
    impl_acquire, impl_column_index_for_row, impl_column_index_for_statement,
    impl_into_arguments_for_arguments,
};
use statement::ExaStatement;

impl_into_arguments_for_arguments!(ExaArguments);
impl_acquire!(Exasol, ExaConnection);
impl_column_index_for_row!(ExaRow);
impl_column_index_for_statement!(ExaStatement);
