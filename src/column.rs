use std::{borrow::Cow, fmt::Display, sync::Arc};

use serde::{Deserialize, Deserializer, Serialize};
use sqlx_core::{column::Column, database::Database};

use crate::{database::Exasol, type_info::ExaTypeInfo};

#[derive(Deserialize)]
#[serde(transparent)]
pub(crate) struct ExaColumnsDe(pub Vec<ExaColumn>);

#[derive(Debug, Deserialize)]
#[serde(from = "ExaColumnsDe")]
pub(crate) struct ExaColumns(pub Arc<[ExaColumn]>);

impl From<ExaColumnsDe> for ExaColumns {
    fn from(mut value: ExaColumnsDe) -> Self {
        value
            .0
            .iter_mut()
            .enumerate()
            .for_each(|(idx, c)| c.ordinal = idx);

        Self(value.0.into())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExaColumn {
    #[serde(skip)]
    pub(crate) ordinal: usize,
    #[serde(deserialize_with = "ExaColumn::lowercase_name")]
    pub(crate) name: Arc<str>,
    #[serde(rename = "dataType")]
    pub(crate) datatype: ExaTypeInfo,
}

impl ExaColumn {
    fn lowercase_name<'de, D>(deserializer: D) -> Result<Arc<str>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let name = Cow::<str>::deserialize(deserializer)?;
        Ok(Arc::from(name.to_lowercase()))
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
        self.ordinal
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn type_info(&self) -> &<Self::Database as Database>::TypeInfo {
        &self.datatype
    }
}
