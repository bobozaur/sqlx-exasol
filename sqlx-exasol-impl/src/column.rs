use std::{borrow::Cow, fmt::Display};

use serde::{Deserialize, Deserializer, Serialize};
use sqlx_core::{column::Column, database::Database, ext::ustr::UStr};

use crate::{database::Exasol, type_info::ExaTypeInfo};

/// Implementor of [`Column`].
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExaColumn {
    #[serde(default)]
    pub(crate) ordinal: usize,
    #[serde(deserialize_with = "ExaColumn::lowercase_name")]
    pub(crate) name: UStr,
    pub(crate) data_type: ExaTypeInfo,
}

impl ExaColumn {
    fn lowercase_name<'de, D>(deserializer: D) -> Result<UStr, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// Intermediate type used to take advantage of [`Cow`] borrowed deserialization when
        /// possible.
        ///
        /// In regular usage, an owned buffer is used and a borrowed [`str`] could be
        /// used, but for offline query deserialization a reader seems to be used and the buffer is
        /// shortlived, hence a string slice would fail deserialization.
        #[derive(Deserialize)]
        struct CowStr<'a>(#[serde(borrow)] Cow<'a, str>);

        CowStr::deserialize(deserializer)
            .map(|c| c.0.to_lowercase())
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
