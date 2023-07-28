use std::sync::Arc;

use serde::Deserialize;

use crate::column::ExaColumn;

/// Struct representing a prepared statement
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "PreparedStatementDe")]
pub struct PreparedStatement {
    pub(crate) statement_handle: u16,
    pub(crate) columns: Arc<[ExaColumn]>,
}

impl TryFrom<PreparedStatementDe> for PreparedStatement {
    type Error = String;

    fn try_from(value: PreparedStatementDe) -> Result<Self, Self::Error> {
        let mut columns = match value.parameter_data {
            Some(Parameters { columns }) => columns,
            None => Vec::new(),
        };

        columns
            .iter_mut()
            .enumerate()
            .for_each(|(idx, c)| c.ordinal = idx);

        let prepared_stmt = Self {
            statement_handle: value.statement_handle,
            columns: columns.into(),
        };

        Ok(prepared_stmt)
    }
}

/// Struct representing a prepared statement
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PreparedStatementDe {
    statement_handle: u16,
    parameter_data: Option<Parameters>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Parameters {
    columns: Vec<ExaColumn>,
}
