use std::{cmp::Ordering, fmt::Display};

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

impl ExaTypeInfo {
    const BOOLEAN: &str = "BOOLEAN";
    const CHAR: &str = "CHAR";
    const DATE: &str = "DATE";
    const DECIMAL: &str = "DECIMAL";
    const DOUBLE: &str = "DOUBLE PRECISION";
    const GEOMETRY: &str = "GEOMETRY";
    const INTERVAL_DAY_TO_SECOND: &str = "INTERVAL DAY TO SECOND";
    const INTERVAL_YEAR_TO_MONTH: &str = "INTERVAL YEAR TO MONTH";
    const TIMESTAMP: &str = "TIMESTAMP";
    const TIMESTAMP_WITH_LOCAL_TIME_ZONE: &str = "TIMESTAMP WITH LOCAL TIME ZONE";
    const VARCHAR: &str = "VARCHAR";
    const HASHTYPE: &str = "HASHTYPE";

    /// Returns `true` if this instance is compatible with the other one provided.
    pub(crate) fn compatible(&self, other: &Self) -> bool {
        match self {
            ExaTypeInfo::Boolean => matches!(other, ExaTypeInfo::Boolean),
            ExaTypeInfo::Char(c) | ExaTypeInfo::Varchar(c) => c.compatible(other),
            ExaTypeInfo::Date => matches!(
                other,
                ExaTypeInfo::Date | ExaTypeInfo::Char(_) | ExaTypeInfo::Varchar(_)
            ),
            ExaTypeInfo::Decimal(d) => d.compatible(other),
            ExaTypeInfo::Double => match other {
                ExaTypeInfo::Double => true,
                ExaTypeInfo::Decimal(d) if d.scale > 0 => true,
                _ => false,
            },
            ExaTypeInfo::Geometry(g) => g.compatible(other),
            ExaTypeInfo::IntervalDayToSecond(ids) => ids.compatible(other),
            ExaTypeInfo::IntervalYearToMonth(iym) => iym.compatible(other),
            ExaTypeInfo::Timestamp => matches!(
                other,
                ExaTypeInfo::Timestamp
                    | ExaTypeInfo::TimestampWithLocalTimeZone
                    | ExaTypeInfo::Char(_)
                    | ExaTypeInfo::Varchar(_)
            ),
            ExaTypeInfo::TimestampWithLocalTimeZone => matches!(
                other,
                ExaTypeInfo::TimestampWithLocalTimeZone
                    | ExaTypeInfo::Timestamp
                    | ExaTypeInfo::Char(_)
                    | ExaTypeInfo::Varchar(_)
            ),
            ExaTypeInfo::Hashtype(h) => h.compatible(other),
        }
    }
}

impl AsRef<str> for ExaTypeInfo {
    fn as_ref(&self) -> &str {
        match self {
            ExaTypeInfo::Boolean => Self::BOOLEAN,
            ExaTypeInfo::Char(_) => Self::CHAR,
            ExaTypeInfo::Date => Self::DATE,
            ExaTypeInfo::Decimal(_) => Self::DECIMAL,
            ExaTypeInfo::Double => Self::DOUBLE,
            ExaTypeInfo::Geometry(_) => Self::GEOMETRY,
            ExaTypeInfo::IntervalDayToSecond(_) => Self::INTERVAL_DAY_TO_SECOND,
            ExaTypeInfo::IntervalYearToMonth(_) => Self::INTERVAL_YEAR_TO_MONTH,
            ExaTypeInfo::Timestamp => Self::TIMESTAMP,
            ExaTypeInfo::TimestampWithLocalTimeZone => Self::TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            ExaTypeInfo::Varchar(_) => Self::VARCHAR,
            ExaTypeInfo::Hashtype(_) => Self::HASHTYPE,
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
        match self {
            ExaTypeInfo::Boolean
            | ExaTypeInfo::Date
            | ExaTypeInfo::Double
            | ExaTypeInfo::Timestamp
            | ExaTypeInfo::TimestampWithLocalTimeZone => write!(f, "{}", self.as_ref()),
            ExaTypeInfo::Char(c) | ExaTypeInfo::Varchar(c) => {
                write!(f, "{}({}) {}", self.as_ref(), c.size, c.character_set)
            }
            ExaTypeInfo::Decimal(d) => write!(f, "{}({}, {})", self.as_ref(), d.precision, d.scale),
            ExaTypeInfo::Geometry(g) => write!(f, "{}({})", self.as_ref(), g.srid),
            ExaTypeInfo::IntervalDayToSecond(ids) => write!(
                f,
                "INTERVAL DAY ({}) TO SECOND ({})",
                ids.precision, ids.fraction
            ),
            ExaTypeInfo::IntervalYearToMonth(iym) => {
                write!(f, "INTERVAL YEAR ({}) TO MONTH", iym.precision)
            }
            // For HASHTYPE the database returns the size as doubled.
            ExaTypeInfo::Hashtype(h) => write!(f, "{}({} BYTE)", self.as_ref(), h.size / 2),
        }
    }
}

impl Eq for ExaTypeInfo {}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StringLike {
    size: usize,
    character_set: Charset,
}

impl StringLike {
    const MAX_STR_LEN: usize = 2_000_000;

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

    /// Strings are complex and ensuring one fits inside
    /// a database column would imply a lot of overhead.
    ///
    /// So just let the database do its thing and throw an error.
    pub fn compatible(&self, ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Char(_) | ExaTypeInfo::Varchar(_))
    }
}

impl Default for StringLike {
    fn default() -> Self {
        Self {
            size: Self::MAX_STR_LEN,
            character_set: Charset::Utf8,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Charset {
    Utf8,
    Ascii,
}

impl AsRef<str> for Charset {
    fn as_ref(&self) -> &str {
        match self {
            Charset::Utf8 => "UTF8",
            Charset::Ascii => "ASCII",
        }
    }
}

impl Display for Charset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Decimal {
    precision: u32,
    scale: u32,
}

impl Decimal {
    pub const MAX_8BIT_PRECISION: u32 = 3;
    pub const MAX_16BIT_PRECISION: u32 = 5;
    pub const MAX_32BIT_PRECISION: u32 = 10;
    pub const MAX_64BIT_PRECISION: u32 = 20;
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

/// The purpose of this is to be able to tell if some [`Decimal`] fits
/// inside another [`Decimal`].
///
/// Therefore, we consider cases such as:
/// - DECIMAL(10, 1) != DECIMAL(9, 2)
/// - DECIMAL(10, 1) != DECIMAL(10, 2)
/// - DECIMAL(10, 1) < DECIMAL(11, 2)
/// - DECIMAL(10, 1) < DECIMAL(17, 4)
///
/// - DECIMAL(10, 1) > DECIMAL(9, 1)
/// - DECIMAL(10, 1) = DECIMAL(10, 1)
/// - DECIMAL(10, 1) < DECIMAL(11, 1)
///
/// So, our rule will be:
/// - if a.scale > b.scale, a > b if and only if (a.precision - a.scale) >= (b.precision - b.scale)
/// - if a.scale == b.scale, a == b if and only if (a.precision - a.scale) == (b.precision - b.scale)
/// - if a.scale < b.scale, a < b if and only if (a.precision - a.scale) <= (b.precision - b.scale)
impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_diff = self.precision - self.scale;
        let other_diff = other.precision - other.scale;

        let scale_cmp = self.scale.partial_cmp(&other.scale);
        let diff_cmp = self_diff.partial_cmp(&other_diff);

        match (scale_cmp, diff_cmp) {
            (Some(Ordering::Greater), Some(Ordering::Greater)) => Some(Ordering::Greater),
            (Some(Ordering::Greater), Some(Ordering::Equal)) => Some(Ordering::Greater),
            (Some(Ordering::Equal), ord) => ord,
            (Some(Ordering::Less), Some(Ordering::Less)) => Some(Ordering::Less),
            (Some(Ordering::Less), Some(Ordering::Equal)) => Some(Ordering::Less),
            _ => None,
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, PartialOrd)]
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
            ExaTypeInfo::Hashtype(h) => self.size >= h.size,
            ExaTypeInfo::Varchar(_) | ExaTypeInfo::Char(_) => true,
            _ => false,
        }
    }
}
