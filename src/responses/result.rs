use serde::Deserialize;

use crate::column::ExaColumn;

use super::fetched::DataChunk;

/// Struct used for deserialization of the JSON
/// returned after executing one or more queries
/// Represents the collection of results from all queries.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StmtResult {
    pub(crate) results: [QueryResult; 1],
}

/// Struct used for deserialization of the JSON
/// returned sending queries to the database.
/// Represents the result of one query.
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", rename_all = "camelCase")]
pub enum QueryResult {
    #[serde(rename_all = "camelCase")]
    ResultSet { result_set: ResultSet },
    #[serde(rename_all = "camelCase")]
    RowCount { row_count: u64 },
}

impl QueryResult {
    pub fn handle(&self) -> Option<u16> {
        match self {
            QueryResult::ResultSet { result_set } => result_set.result_set_handle,
            QueryResult::RowCount { .. } => None,
        }
    }
}

/// Struct representing a database result set.
/// You'll generally only interact with this if you need information about result set columns.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultSetDe {
    #[serde(rename = "numRows")]
    total_rows_num: usize,
    result_set_handle: Option<u16>,
    columns: Vec<ExaColumn>,
    #[serde(flatten)]
    data_chunk: DataChunk,
}

#[derive(Debug, Deserialize)]
#[serde(from = "ResultSetDe")]
pub struct ResultSet {
    pub(crate) total_rows_num: usize,
    pub(crate) result_set_handle: Option<u16>,
    pub(crate) columns: Vec<ExaColumn>,
    pub(crate) data_chunk: DataChunk,
}

impl From<ResultSetDe> for ResultSet {
    fn from(mut value: ResultSetDe) -> Self {
        value
            .columns
            .iter_mut()
            .enumerate()
            .for_each(|(idx, c)| c.ordinal = idx);

        Self {
            total_rows_num: value.total_rows_num,
            result_set_handle: value.result_set_handle,
            columns: value.columns,
            data_chunk: value.data_chunk,
        }
    }
}
