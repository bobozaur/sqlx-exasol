use std::fmt::Display;

use serde::{Deserialize, Serialize};
use sqlx::Column;

use crate::{type_info::ExaTypeInfo, database::Exasol};

#[allow(unused)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExaColumn {
    name: String,
    #[serde(rename = "dataType")]
    datatype: ExaTypeInfo,
}

impl ExaColumn {
    /// Sets the column name
    /// Could be used for changing the deserialization name to match
    /// a certain struct without resorting to Serde
    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    /// Returns a reference to the column name
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns a reference to the column datatype
    pub fn datatype(&self) -> &ExaTypeInfo {
        &self.datatype
    }

    /// Turns the name of this column into lowercase
    pub(crate) fn use_lowercase_name(&mut self) {
        self.set_name(self.name.to_lowercase())
    }
}

impl Display for ExaColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.datatype)
    }
}

impl Column for ExaColumn {
    type Database = Exasol;

    fn ordinal(&self) -> usize {
        todo!()
    }

    fn name(&self) -> &str {
        todo!()
    }

    fn type_info(&self) -> &<Self::Database as sqlx::Database>::TypeInfo {
        todo!()
    }
}