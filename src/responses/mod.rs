//! Module containing data structures used in representing data returned from the database.

mod attributes;
mod columns;
mod describe;
mod error;
mod fetch;
mod hosts;
mod prepared_stmt;
mod public_key;
mod result;
mod session_info;

use std::fmt;

use serde::{
    de::{SeqAccess, Visitor},
    Deserialize, Deserializer,
};

pub use attributes::{Attributes, ExaAttributes};
pub use describe::DescribeStatement;
pub use error::ExaDatabaseError;
pub use fetch::DataChunk;
pub use hosts::Hosts;
pub use prepared_stmt::PreparedStatement;
pub use public_key::PublicKey;
pub use result::{QueryResult, ResultSet, ResultSetOutput, Results};
pub use session_info::SessionInfo;

use crate::ExaTypeInfo;

use self::columns::ExaColumns;

/// A response from the Exasol server.
#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum Response<T> {
    #[serde(rename_all = "camelCase")]
    Ok {
        response_data: T,
        attributes: Option<Attributes>,
    },
    Error {
        exception: ExaDatabaseError,
    },
}

/// Enum representing the columns output of a [`PreparedStatement`].
/// It is structured like this because we basically get a result set like
/// construct, but only the columns are relevant.
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", rename_all = "camelCase")]
enum OutputColumns {
    #[serde(rename_all = "camelCase")]
    ResultSet {
        result_set: ColumnSet,
    },
    RowCount {},
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColumnSet {
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
