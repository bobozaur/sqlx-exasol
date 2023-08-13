use std::fmt;

use serde::{
    de::{SeqAccess, Visitor},
    Deserialize, Deserializer,
};

use crate::{ExaColumn, ExaTypeInfo};

use super::columns::ExaColumns;

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
                QueryResult::ResultSet { result_set } => result_set.columns.0,
                QueryResult::RowCount {} => Vec::new(),
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
    results: Option<[QueryResult; 1]>,
}

/// Struct representing the result of a single query.
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", rename_all = "camelCase")]
enum QueryResult {
    #[serde(rename_all = "camelCase")]
    ResultSet {
        result_set: ResultSet,
    },
    RowCount {},
}

/// Deserialization helper for [`ResultSet`].
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultSet {
    columns: ExaColumns,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Parameters {
    #[serde(deserialize_with = "Parameters::datatype_from_column")]
    columns: Vec<ExaTypeInfo>,
}

impl Parameters {
    /// Helper allowing us to deserialize what would be an [`ExaColumn`]
    /// directly to an [`ExaTypeInfo`].
    fn datatype_from_column<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<ExaTypeInfo>, D::Error> {
        struct TypeInfoVisitor;

        impl<'de> Visitor<'de> for TypeInfoVisitor {
            type Value = Vec<ExaTypeInfo>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "An array of arrays")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut transposed = Vec::new();

                while let Some(parameter) = seq.next_element::<ExaParameterType>()? {
                    transposed.push(parameter.data_type)
                }
                Ok(transposed)
            }
        }

        deserializer.deserialize_seq(TypeInfoVisitor)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExaParameterType {
    data_type: ExaTypeInfo,
}
