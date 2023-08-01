use std::{cmp::Ordering, fmt::Display};

use serde::{Deserialize, Serialize};
use sqlx_core::{type_info::TypeInfo, types::Type, Error as SqlxError};

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

impl ExaTypeInfo {
    pub fn check_compatibility(&self, ty: &Self) -> Result<(), SqlxError> {
        let flag = match self {
            ExaTypeInfo::Boolean => bool::compatible(ty),
            ExaTypeInfo::Char(c) | ExaTypeInfo::Varchar(c) => c.compatible(ty),
            ExaTypeInfo::Date => matches!(
                ty,
                ExaTypeInfo::Date | ExaTypeInfo::Char(_) | ExaTypeInfo::Varchar(_)
            ),
            ExaTypeInfo::Decimal(d) => d.compatible(ty),
            ExaTypeInfo::Double => match ty {
                ExaTypeInfo::Double => true,
                ExaTypeInfo::Decimal(d) if d.scale > 0 => true,
                _ => false,
            },
            ExaTypeInfo::Geometry(g) => g.compatible(ty),
            ExaTypeInfo::IntervalDayToSecond(ids) => ids.compatible(ty),
            ExaTypeInfo::IntervalYearToMonth(iym) => iym.compatible(ty),
            ExaTypeInfo::Timestamp => matches!(
                ty,
                ExaTypeInfo::Timestamp
                    | ExaTypeInfo::TimestampWithLocalTimeZone
                    | ExaTypeInfo::Char(_)
                    | ExaTypeInfo::Varchar(_)
            ),
            ExaTypeInfo::TimestampWithLocalTimeZone => matches!(
                ty,
                ExaTypeInfo::TimestampWithLocalTimeZone
                    | ExaTypeInfo::Timestamp
                    | ExaTypeInfo::Char(_)
                    | ExaTypeInfo::Varchar(_)
            ),
            ExaTypeInfo::Hashtype(h) => h.compatible(ty),
        };

        if flag {
            return Ok(());
        }

        let msg = format!("type mismatch: expected SQL type `{self}` but was provided `{ty}`");
        Err(SqlxError::Decode(msg.into()))
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

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, PartialOrd)]
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

    pub fn compatible(&self, ty: &ExaTypeInfo) -> bool {
        match ty {
            ExaTypeInfo::Char(c) | ExaTypeInfo::Varchar(c) => self >= c,
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Charset {
    #[default]
    Utf8,
    Ascii,
}

impl PartialOrd for Charset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Charset::Utf8, Charset::Utf8) => Some(Ordering::Equal),
            (Charset::Utf8, Charset::Ascii) => Some(Ordering::Equal),
            (Charset::Ascii, Charset::Utf8) => Some(Ordering::Less),
            (Charset::Ascii, Charset::Ascii) => Some(Ordering::Equal),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub struct Decimal {
    precision: u32,
    scale: u32,
}

impl Decimal {
    pub const MAX_8BIT_PRECISION: u32 = 3;
    pub const MAX_16BIT_PRECISION: u32 = 5;
    pub const MAX_32BIT_PRECISION: u32 = 10;
    pub const MAX_64BIT_PRECISION: u32 = 18;
    pub const MAX_PRECISION: u32 = 36;
    pub const MAX_SCALE: u32 = 35;

    pub fn new(precision: u32, scale: u32) -> Self {
        Self { precision, scale }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    pub fn compatible(&self, ty: &ExaTypeInfo) -> bool {
        match ty {
            ExaTypeInfo::Decimal(d) => self >= d,
            ExaTypeInfo::Double => self.scale > 0,
            _ => false,
        }
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

    pub fn compatible(&self, ty: &ExaTypeInfo) -> bool {
        match ty {
            ExaTypeInfo::Geometry(g) => self.srid == g.srid,
            ExaTypeInfo::Varchar(_) | ExaTypeInfo::Char(_) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, PartialOrd)]
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

    pub fn compatible(&self, ty: &ExaTypeInfo) -> bool {
        match ty {
            ExaTypeInfo::IntervalDayToSecond(i) => self >= i,
            ExaTypeInfo::Varchar(_) | ExaTypeInfo::Char(_) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, PartialOrd)]
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

    pub fn compatible(&self, ty: &ExaTypeInfo) -> bool {
        match ty {
            ExaTypeInfo::IntervalYearToMonth(i) => self >= i,
            ExaTypeInfo::Varchar(_) | ExaTypeInfo::Char(_) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Hashtype {
    size: usize,
}

impl Default for Hashtype {
    fn default() -> Self {
        Self { size: 32 }
    }
}

impl Hashtype {
    pub fn new(size: usize) -> Self {
        Self { size }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn compatible(&self, ty: &ExaTypeInfo) -> bool {
        match ty {
            ExaTypeInfo::Varchar(s) | ExaTypeInfo::Char(s) => self.size >= s.size,
            ExaTypeInfo::Hashtype(h) => self.size >= h.size,
            _ => false,
        }
    }
}
