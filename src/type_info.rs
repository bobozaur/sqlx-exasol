use std::fmt::Display;

use serde::{Deserialize, Serialize};
use sqlx_core::type_info::TypeInfo;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExaTypeInfo {
    #[serde(rename = "type")]
    pub(crate) data_type: DataType,
    precision: Option<u8>,
    scale: Option<u8>,
    size: Option<usize>,
    character_set: Option<String>,
    with_local_time_zone: Option<bool>,
    fraction: Option<usize>,
    srid: Option<usize>,
}

impl ExaTypeInfo {
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
            srid: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataType {
    Null,
    Boolean,
    Char,
    Date,
    Decimal,
    DoublePrecision,
    Geometry,
    IntervalDay,
    IntervalYear,
    Timestamp,
    TimestampWithLocalTimeZone,
    Varchar,
    Hashtype,
}

impl AsRef<str> for DataType {
    fn as_ref(&self) -> &str {
        match self {
            DataType::Null => "NULL",
            DataType::Boolean => "BOOLEAN",
            DataType::Char => "CHAR",
            DataType::Date => "DATE",
            DataType::Decimal => "DECIMAL",
            DataType::DoublePrecision => "DOUBLE PRECISION",
            DataType::Geometry => todo!(),
            DataType::IntervalDay => todo!(),
            DataType::IntervalYear => todo!(),
            DataType::Timestamp => "TIMESTAMP",
            DataType::TimestampWithLocalTimeZone => todo!(),
            DataType::Varchar => "VARCHAR",
            DataType::Hashtype => todo!(),
        }
    }
}

impl TypeInfo for ExaTypeInfo {
    fn is_null(&self) -> bool {
        matches!(self.data_type, DataType::Null)
    }

    fn name(&self) -> &str {
        self.data_type.as_ref()
    }
}

impl Display for ExaTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data_type.as_ref())
    }
}

impl PartialEq<ExaTypeInfo> for ExaTypeInfo {
    fn eq(&self, other: &ExaTypeInfo) -> bool {
        self.data_type == other.data_type
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
