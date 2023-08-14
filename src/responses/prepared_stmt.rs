use std::sync::Arc;

use serde::Deserialize;

use crate::{column::ExaColumn, ExaTypeInfo};

use super::{OutputColumns, Parameters};

/// Struct representing a prepared statement handle and column parameters metadata.
#[derive(Clone, Debug, Deserialize)]
#[serde(from = "PreparedStatementDe")]
pub struct PreparedStatement {
    pub(crate) statement_handle: u16,
    pub(crate) columns: Arc<[ExaColumn]>,
    pub(crate) parameters: Arc<[ExaTypeInfo]>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PreparedStatementDe {
    statement_handle: u16,
    parameter_data: Option<Parameters>,
    results: Option<[OutputColumns; 1]>,
}

impl From<PreparedStatementDe> for PreparedStatement {
    fn from(value: PreparedStatementDe) -> Self {
        let columns = match value.results {
            Some(arr) => match arr.into_iter().next().unwrap() {
                OutputColumns::ResultSet { result_set } => result_set.columns.0.into(),
                OutputColumns::RowCount {} => Vec::new().into(),
            },
            None => Vec::new().into(),
        };

        let parameters = match value.parameter_data {
            Some(Parameters { columns }) => columns.into(),
            None => Vec::new().into(),
        };

        Self {
            statement_handle: value.statement_handle,
            parameters,
            columns,
        }
    }
}
