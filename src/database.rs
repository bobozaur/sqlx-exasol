use serde_json::Value;
use sqlx_core::database::{Database, HasArguments, HasStatement, HasStatementCache, HasValueRef};

use crate::{
    arguments::ExaArguments,
    column::ExaColumn,
    connection::ExaConnection,
    query_result::ExaQueryResult,
    row::ExaRow,
    statement::ExaStatement,
    transaction::ExaTransactionManager,
    type_info::ExaTypeInfo,
    value::{ExaValue, ExaValueRef},
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

    const URL_SCHEMES: &'static [&'static str] = &["exa"];
}

impl<'r> HasValueRef<'r> for Exasol {
    type Database = Exasol;

    type ValueRef = ExaValueRef<'r>;
}

impl HasArguments<'_> for Exasol {
    type Database = Exasol;

    type Arguments = ExaArguments;

    type ArgumentBuffer = Vec<[Value; 1]>;
}

impl<'q> HasStatement<'q> for Exasol {
    type Database = Exasol;

    type Statement = ExaStatement<'q>;
}

impl HasStatementCache for Exasol {}
