use sqlx::{
    database::{HasArguments, HasStatement, HasStatementCache, HasValueRef},
    Database,
};

use crate::{
    arguments::{ExaArgumentValue, ExaArguments},
    connection::ExaConnection,
    query_result::ExaQueryResult,
    row::ExaRow,
    statement::ExaStatement,
    transaction::ExaTransactionManager,
    type_info::ExaTypeInfo,
    value::{ExaValue, ExaValueRef}, column::ExaColumn,
};

#[derive(Debug)]
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

    const URL_SCHEMES: &'static [&'static str] = &[];
}

impl<'r> HasValueRef<'r> for Exasol {
    type Database = Exasol;

    type ValueRef = ExaValueRef<'r>;
}

impl HasArguments<'_> for Exasol {
    type Database = Exasol;

    type Arguments = ExaArguments;

    type ArgumentBuffer = Vec<ExaArgumentValue>;
}

impl<'q> HasStatement<'q> for Exasol {
    type Database = Exasol;

    type Statement = ExaStatement<'q>;
}

impl HasStatementCache for Exasol {}
