use sqlx_core::database::{Database, HasStatementCache};

use crate::{
    arguments::{ExaArguments, ExaBuffer},
    column::ExaColumn,
    connection::ExaConnection,
    query_result::ExaQueryResult,
    row::ExaRow,
    statement::ExaStatement,
    transaction::ExaTransactionManager,
    type_info::ExaTypeInfo,
    value::{ExaValue, ExaValueRef},
};

#[derive(Debug, Clone, Copy)]
pub struct Exasol;

impl Database for Exasol {
    type Connection = ExaConnection;

    type TransactionManager = ExaTransactionManager;

    type Row = ExaRow;

    type QueryResult = ExaQueryResult;

    type Column = ExaColumn;

    type TypeInfo = ExaTypeInfo;

    type Value = ExaValue;

    const NAME: &'static str = "Exasol";

    const URL_SCHEMES: &'static [&'static str] = &["exa"];

    type ValueRef<'r> = ExaValueRef<'r>;

    type Arguments<'q> = ExaArguments;

    type ArgumentBuffer<'q> = ExaBuffer;

    type Statement<'q> = ExaStatement<'q>;
}

impl HasStatementCache for Exasol {}
