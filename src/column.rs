use std::{fmt::Display, sync::Arc};

use serde::{Deserialize, Deserializer};
use sqlx_core::{column::Column, database::Database};

use crate::{database::Exasol, type_info::ExaTypeInfo};

/// Implementor of [`Column`].
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExaColumn {
    #[serde(skip)]
    pub(crate) ordinal: usize,
    #[serde(deserialize_with = "ExaColumn::lowercase_name")]
    pub(crate) name: Arc<str>,
    pub(crate) data_type: ExaTypeInfo,
}

impl ExaColumn {
    fn lowercase_name<'de, D>(deserializer: D) -> Result<Arc<str>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // NOTE: We can borrow because we always deserialize from an owned buffer.
        <&str>::deserialize(deserializer)
            .map(str::to_lowercase)
            .map(From::from)
    }
}

impl Display for ExaColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.data_type)
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
        &self.data_type
    }
}
