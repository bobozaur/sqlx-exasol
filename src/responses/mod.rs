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

pub use attributes::{ExaAttributes, ExaAttributesOpt, ExaRwAttributes};
pub use describe::DescribeStatement;
pub use error::ExaDatabaseError;
pub use fetch::DataChunk;
#[cfg(feature = "etl")]
pub use hosts::Hosts;
pub use prepared_stmt::PreparedStatement;
pub use public_key::PublicKey;
pub use result::{MultiResults, QueryResult, ResultSet, ResultSetOutput, SingleResult};
use serde::{
    de::{DeserializeSeed, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::Value;
pub use session_info::SessionInfo;

use self::columns::ExaColumns;
use crate::ExaTypeInfo;

/// A response from the Exasol server.
#[derive(Debug, Deserialize)]
#[serde(tag = "status")]
#[serde(rename_all = "camelCase")]
pub enum ExaResult<T> {
    #[serde(rename_all = "camelCase")]
    Ok {
        response_data: T,
        attributes: Option<ExaAttributesOpt>,
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
#[serde(tag = "resultType")]
#[serde(rename_all = "camelCase")]
enum OutputColumns {
    #[serde(rename_all = "camelCase")]
    ResultSet {
        result_set: ResultSetColumns,
    },
    RowCount {},
}

/// Deserialization helper.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResultSetColumns {
    columns: ExaColumns,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExaParameterType {
    data_type: ExaTypeInfo,
}

/// The `parameter_data` field of a [`PreparedStatement`].
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Parameters {
    #[serde(deserialize_with = "Parameters::datatype_from_column")]
    columns: Vec<ExaTypeInfo>,
}

impl Parameters {
    /// Helper allowing us to deserialize what would be an [`crate::ExaColumn`]
    /// directly to an [`ExaTypeInfo`] as sometimes the other information is redundant.
    fn datatype_from_column<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<ExaTypeInfo>, D::Error> {
        struct TypeInfoVisitor;

        impl<'de> Visitor<'de> for TypeInfoVisitor {
            type Value = Vec<ExaTypeInfo>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "An array of arrays")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut transposed = Vec::new();

                while let Some(parameter) = seq.next_element::<ExaParameterType>()? {
                    transposed.push(parameter.data_type);
                }

                Ok(transposed)
            }
        }

        deserializer.deserialize_seq(TypeInfoVisitor)
    }
}

/// Deserialization function used to turn Exasol's column major data into row major.
fn to_row_major<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Vec<Value>>, D::Error> {
    struct ColumnVisitor<'a>(&'a mut Vec<Vec<Value>>);

    impl<'de> Visitor<'de> for ColumnVisitor<'_> {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "An array")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<(), A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut index = 0;

            while let Some(elem) = seq.next_element()? {
                if let Some(row) = self.0.get_mut(index) {
                    row.push(elem);
                } else {
                    self.0.push(vec![elem]);
                }

                index += 1;
            }
            Ok(())
        }
    }

    struct Columns<'a>(&'a mut Vec<Vec<Value>>);

    impl<'de> DeserializeSeed<'de> for Columns<'_> {
        type Value = ();

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_seq(ColumnVisitor(self.0))
        }
    }

    struct DataVisitor;

    impl<'de> Visitor<'de> for DataVisitor {
        type Value = Vec<Vec<Value>>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "An array of arrays")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut transposed = Vec::new();

            while seq.next_element_seed(Columns(&mut transposed))?.is_some() {}
            Ok(transposed)
        }
    }

    deserializer.deserialize_seq(DataVisitor)
}
