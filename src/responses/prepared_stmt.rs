use std::sync::Arc;

use serde::Deserialize;

use crate::column::ExaColumn;

use super::result::ResultSet;

/// Struct representing a prepared statement
#[derive(Clone, Debug, Deserialize)]
#[serde(from = "PreparedStatementDe")]
pub struct PreparedStatement {
    pub(crate) statement_handle: u16,
    pub(crate) columns: Arc<[ExaColumn]>,
}

impl From<PreparedStatementDe> for PreparedStatement {
    fn from(value: PreparedStatementDe) -> Self {
        let mut columns = match value.parameters {
            Parameters::ParameterData { columns } => columns,
            Parameters::Results(res) => res.into_iter().next().unwrap().result_set.columns,
        };

        columns
            .iter_mut()
            .enumerate()
            .for_each(|(idx, c)| c.ordinal = idx);

        Self {
            statement_handle: value.statement_handle,
            columns: columns.into(),
        }
    }
}

/// Struct representing a prepared statement
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PreparedStatementDe {
    statement_handle: u16,
    parameters: Parameters,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Parameters {
    ParameterData { columns: Vec<ExaColumn> },
    Results([ResultParams; 1]),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultParams {
    result_set: ResultSet,
}
