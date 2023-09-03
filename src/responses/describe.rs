use serde::Deserialize;

use super::{OutputColumns, Parameters};
use crate::{ExaColumn, ExaTypeInfo};

#[derive(Clone, Debug, Deserialize)]
#[serde(from = "DescribeStatementDe")]
pub struct DescribeStatement {
    pub(crate) statement_handle: u16,
    pub(crate) columns: Vec<ExaColumn>,
    pub(crate) parameters: Vec<ExaTypeInfo>,
}

impl From<DescribeStatementDe> for DescribeStatement {
    fn from(value: DescribeStatementDe) -> Self {
        let columns = match value.results {
            Some(arr) => match arr.into_iter().next().unwrap() {
                OutputColumns::ResultSet { result_set } => result_set.columns.0,
                OutputColumns::RowCount {} => Vec::new(),
            },
            None => Vec::new(),
        };

        let parameters = value.parameter_data.map(|p| p.columns).unwrap_or_default();

        Self {
            columns,
            parameters,
            statement_handle: value.statement_handle,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeStatementDe {
    statement_handle: u16,
    parameter_data: Option<Parameters>,
    results: Option<[OutputColumns; 1]>,
}
