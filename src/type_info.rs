use std::{default, fmt::Display};

use serde::{Deserialize, Serialize};
use sqlx_core::type_info::TypeInfo;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
#[serde(tag = "type")]
pub enum ExaTypeInfo {
    Boolean,
    Char(StringLike),
    Date,
    Decimal(Decimal),
    Double,
    Geometry(Geometry),
    #[serde(rename = "INTERVAL DAY TO SECOND")]
    IntervalDayToSecond(IntervalDayToSecond),
    #[serde(rename = "INTERVAL YEAR TO MONTH")]
    IntervalYearToMonth(IntervalYearToMonth),
    Timestamp,
    #[serde(rename = "TIMESTAMP WITH LOCAL TIME ZONE")]
    TimestampWithLocalTimeZone,
    Varchar(StringLike),
    Hashtype(Hashtype),
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StringLike {
    size: usize,
    character_set: Charset,
}

impl StringLike {
    pub fn new(size: usize, character_set: Charset) -> Self {
        Self {
            size,
            character_set,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn character_set(&self) -> Charset {
        self.character_set
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Charset {
    #[default]
    Utf8,
    Ascii,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Decimal {
    precision: u32,
    scale: u32,
}

impl Decimal {
    pub fn new(precision: u32, scale: u32) -> Self {
        Self { precision, scale }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Geometry {
    srid: u16,
}

impl Geometry {
    pub fn new(srid: u16) -> Self {
        Self { srid }
    }

    pub fn srid(&self) -> u16 {
        self.srid
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IntervalDayToSecond {
    precision: u32,
    fraction: u32,
}

impl Default for IntervalDayToSecond {
    fn default() -> Self {
        Self {
            precision: 2,
            fraction: 3,
        }
    }
}

impl IntervalDayToSecond {
    pub fn new(precision: u32, fraction: u32) -> Self {
        Self {
            precision,
            fraction,
        }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn fraction(&self) -> u32 {
        self.fraction
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IntervalYearToMonth {
    precision: u32,
}

impl Default for IntervalYearToMonth {
    fn default() -> Self {
        Self { precision: 2 }
    }
}

impl IntervalYearToMonth {
    pub fn new(precision: u32) -> Self {
        Self { precision }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Hashtype {
    size: u16,
}

impl Default for Hashtype {
    fn default() -> Self {
        Self { size: 32 }
    }
}

impl Hashtype {
    pub fn new(size: u16) -> Self {
        Self { size }
    }

    pub fn size(&self) -> u16 {
        self.size
    }
}

impl AsRef<str> for ExaTypeInfo {
    fn as_ref(&self) -> &str {
        match self {
            ExaTypeInfo::Boolean => "BOOLEAN",
            ExaTypeInfo::Char(_) => "CHAR",
            ExaTypeInfo::Date => "DATE",
            ExaTypeInfo::Decimal(_) => "DECIMAL",
            ExaTypeInfo::Double => "DOUBLE PRECISION",
            ExaTypeInfo::Geometry(_) => "GEOMETRY",
            ExaTypeInfo::IntervalDayToSecond(_) => "INTERVAL DAY TO SECOND",
            ExaTypeInfo::IntervalYearToMonth(_) => "INTERVAL YEAR TO MONTH",
            ExaTypeInfo::Timestamp => "TIMESTAMP",
            ExaTypeInfo::TimestampWithLocalTimeZone => "TIMESTAMP WITH LOCAL TIME ZONE",
            ExaTypeInfo::Varchar(_) => "VARCHAR",
            ExaTypeInfo::Hashtype(_) => "HASHTYPE",
        }
    }
}

impl TypeInfo for ExaTypeInfo {
    fn is_null(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        self.as_ref()
    }
}

impl Display for ExaTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl Eq for ExaTypeInfo {}
