use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value;

use super::{columns::ExaColumns, to_row_major};
use crate::{column::ExaColumn, error::ExaProtocolError};

/// The `results` field returned by Exasol after executing statements.
/// This type is meant to accommodate the circumstance where we're batch executing SQL statements.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MultiResults {
    pub results: Vec<QueryResult>,
}

/// The `results` field returned by Exasol after executing a statement.
/// This type represents a single result originating from a single statement, so we only ever expect
/// a single item in the array.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SingleResult {
    pub results: [QueryResult; 1],
}

impl From<SingleResult> for QueryResult {
    fn from(value: SingleResult) -> Self {
        value
            .results
            .into_iter()
            .next()
            .expect("query result array must have one element")
    }
}

/// Struct representing the result of a query.
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType")]
#[serde(rename_all = "camelCase")]
pub enum QueryResult {
    #[serde(rename_all = "camelCase")]
    ResultSet { result_set: ResultSet },
    #[serde(rename_all = "camelCase")]
    RowCount { row_count: u64 },
}

impl QueryResult {
    pub fn handle(&self) -> Option<u16> {
        let result_set = match self {
            QueryResult::ResultSet { result_set } => result_set,
            QueryResult::RowCount { .. } => return None,
        };

        match result_set.output {
            ResultSetOutput::Handle(handle) => Some(handle),
            ResultSetOutput::Data(_) => None,
        }
    }
}

/// Struct representing a database result set.
#[derive(Debug, Deserialize)]
#[serde(try_from = "ResultSetDe")]
pub struct ResultSet {
    pub total_rows_num: usize,
    pub total_rows_pos: usize,
    pub output: ResultSetOutput,
    pub columns: Arc<[ExaColumn]>,
}

impl TryFrom<ResultSetDe> for ResultSet {
    type Error = ExaProtocolError;

    fn try_from(value: ResultSetDe) -> Result<Self, Self::Error> {
        let data = match value.result_set_handle {
            None => ResultSetOutput::Data(value.data),
            Some(handle) => ResultSetOutput::Handle(handle),
        };

        let result_set = Self {
            total_rows_num: value.num_rows,
            total_rows_pos: value.num_rows_in_message,
            output: data,
            columns: value.columns.0.into(),
        };

        Ok(result_set)
    }
}

/// A result set's data.
/// Exasol will send all the data if the query outputs less than [1000 rows](<https://github.com/exasol/websocket-api/blob/master/docs/commands/executeV1.md>).
/// Otherwise, it returns a handle using which the data can be fetched.
#[derive(Debug)]
pub enum ResultSetOutput {
    Handle(u16),
    Data(Vec<Vec<Value>>),
}

/// Deserialization helper for [`ResultSet`].
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultSetDe {
    num_rows: usize,
    result_set_handle: Option<u16>,
    #[serde(default)]
    #[serde(deserialize_with = "to_row_major")]
    data: Vec<Vec<Value>>,
    columns: ExaColumns,
    num_rows_in_message: usize,
}
