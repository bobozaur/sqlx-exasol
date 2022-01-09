use serde::{Deserialize, Serialize};

use crate::column::ExaColumn;

/// Struct representing a prepared statement
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedStatement {
    statement_handle: u16,
    parameter_data: Option<ParameterData>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParameterData {
    num_columns: u8,
    columns: Vec<ExaColumn>,
}
