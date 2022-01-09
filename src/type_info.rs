use std::fmt::Display;

use serde::{Deserialize, Serialize};
use sqlx::TypeInfo;

/// Struct representing a datatype for a column in a result set.
#[allow(unused)]
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExaTypeInfo {
    #[serde(rename = "type")]
    type_name: String,
    precision: Option<u8>,
    scale: Option<u8>,
    size: Option<usize>,
    character_set: Option<String>,
    with_local_time_zone: Option<bool>,
    fraction: Option<usize>,
    srid: Option<usize>,
}

impl Display for ExaTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.type_name)
    }
}

impl TypeInfo for ExaTypeInfo {
    fn is_null(&self) -> bool {
        self.type_name.to_lowercase() == "null"
    }

    fn name(&self) -> &str {
        &self.type_name
    }
}

impl PartialEq<ExaTypeInfo> for ExaTypeInfo {
    fn eq(&self, other: &ExaTypeInfo) -> bool {
        self.type_name == other.type_name
            && self.precision == other.precision
            && self.scale == other.scale
            && self.size == other.size
            && self.character_set == other.character_set
            && self.with_local_time_zone == other.with_local_time_zone
            && self.fraction == other.fraction
            && self.srid == other.srid
    }
}

impl Eq for ExaTypeInfo {}
