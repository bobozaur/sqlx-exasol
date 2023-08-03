use std::sync::Arc;

use serde::Deserialize;

use crate::column::ExaColumn;

use super::columns::ExaColumns;

/// Struct representing a prepared statement handle and column parameters metadata.
#[derive(Clone, Debug, Deserialize)]
#[serde(from = "PreparedStatementDe")]
pub struct PreparedStatement {
    pub(crate) statement_handle: u16,
    pub(crate) columns: Arc<[ExaColumn]>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedStatementDe {
    statement_handle: u16,
    parameter_data: Option<Parameters>,
}

impl From<PreparedStatementDe> for PreparedStatement {
    fn from(value: PreparedStatementDe) -> Self {
        let columns = match value.parameter_data {
            Some(Parameters { columns }) => columns.0.into(),
            None => Vec::new().into(),
        };

        Self {
            statement_handle: value.statement_handle,
            columns,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Parameters {
    pub(crate) columns: ExaColumns,
}
