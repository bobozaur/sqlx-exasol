use serde::Deserialize;

use crate::column::ExaColumn;

use super::fetched::FetchedData;

/// Struct used for deserialization of the JSON
/// returned after executing one or more queries
/// Represents the collection of results from all queries.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Results {
    num_results: u8,
    results: Vec<QueryResult>,
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
    RowCount { row_count: usize },
}

/// Struct representing a database result set.
/// You'll generally only interact with this if you need information about result set columns.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultSet {
    #[serde(rename = "numRows")]
    total_rows_num: usize,
    result_set_handle: Option<u16>,
    num_columns: usize,
    columns: Vec<ExaColumn>,
    #[serde(flatten)]
    fetched: FetchedData,
}
