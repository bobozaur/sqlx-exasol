use std::{fmt::Display, sync::Arc};

use serde::{Deserialize, Serialize};
use sqlx_core::{column::Column, database::Database};

use crate::{database::Exasol, type_info::ExaTypeInfo};

#[allow(unused)]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExaColumn {
    #[serde(skip)]
    pub(crate) ordinal: usize,
    pub(crate) name: Arc<str>,
    #[serde(rename = "dataType")]
    pub(crate) datatype: ExaTypeInfo,
}

impl Display for ExaColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.datatype)
    }
}

impl Column for ExaColumn {
    type Database = Exasol;

    fn ordinal(&self) -> usize {
        self.ordinal
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn type_info(&self) -> &<Self::Database as Database>::TypeInfo {
        &self.datatype
    }
}
