use std::{
    cmp::Ordering,
    fmt::{Arguments, Display},
};

use arrayvec::ArrayString;
use serde::{Deserialize, Serialize};
use sqlx_core::type_info::TypeInfo;

#[derive(Debug, Clone, Deserialize)]
#[serde(from = "ExaDataType")]
pub struct ExaTypeInfo {
    name: DataTypeName,
    datatype: ExaDataType,
}

impl ExaTypeInfo {
    pub fn compatible(&self, other: &Self) -> bool {
        self.datatype.compatible(&other.datatype)
    }
}

impl From<ExaDataType> for ExaTypeInfo {
    fn from(datatype: ExaDataType) -> Self {
        let name = datatype.full_name();
        Self { name, datatype }
    }
}

impl Serialize for ExaTypeInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.datatype.serialize(serializer)
    }
}

impl PartialEq for ExaTypeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.datatype == other.datatype
    }
}

impl Display for ExaTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl TypeInfo for ExaTypeInfo {
    fn is_null(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        self.name.as_ref()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
#[serde(tag = "type")]
pub enum ExaDataType {
    Null,
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

impl ExaDataType {
    const NULL: &str = "NULL";
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
            ExaDataType::Null => true,
            ExaDataType::Boolean => matches!(other, ExaDataType::Boolean | ExaDataType::Null),
            ExaDataType::Char(c) | ExaDataType::Varchar(c) => c.compatible(other),
            ExaDataType::Date => matches!(
                other,
                ExaDataType::Date
                    | ExaDataType::Char(_)
                    | ExaDataType::Varchar(_)
                    | ExaDataType::Null
            ),
            ExaDataType::Decimal(d) => d.compatible(other),
            ExaDataType::Double => match other {
                ExaDataType::Double | ExaDataType::Null => true,
                ExaDataType::Decimal(d) if d.scale > 0 => true,
                _ => false,
            },
            ExaDataType::Geometry(g) => g.compatible(other),
            ExaDataType::IntervalDayToSecond(ids) => ids.compatible(other),
            ExaDataType::IntervalYearToMonth(iym) => iym.compatible(other),
            ExaDataType::Timestamp => matches!(
                other,
                ExaDataType::Timestamp
                    | ExaDataType::TimestampWithLocalTimeZone
                    | ExaDataType::Char(_)
                    | ExaDataType::Varchar(_)
                    | ExaDataType::Null
            ),
            ExaDataType::TimestampWithLocalTimeZone => matches!(
                other,
                ExaDataType::TimestampWithLocalTimeZone
                    | ExaDataType::Timestamp
                    | ExaDataType::Char(_)
                    | ExaDataType::Varchar(_)
                    | ExaDataType::Null
            ),
            ExaDataType::Hashtype(h) => h.compatible(other),
        }
    }

    fn full_name(&self) -> DataTypeName {
        match self {
            ExaDataType::Null => Self::NULL.into(),
            ExaDataType::Boolean => Self::BOOLEAN.into(),
            ExaDataType::Date => Self::DATE.into(),
            ExaDataType::Double => Self::DOUBLE.into(),
            ExaDataType::Timestamp => Self::TIMESTAMP.into(),
            ExaDataType::TimestampWithLocalTimeZone => Self::TIMESTAMP_WITH_LOCAL_TIME_ZONE.into(),
            ExaDataType::Char(c) | ExaDataType::Varchar(c) => {
                format_args!("{}({}) {}", self.as_ref(), c.size, c.character_set).into()
            }
            ExaDataType::Decimal(d) => {
                format_args!("{}({}, {})", self.as_ref(), d.precision, d.scale).into()
            }
            ExaDataType::Geometry(g) => format_args!("{}({})", self.as_ref(), g.srid).into(),
            ExaDataType::IntervalDayToSecond(ids) => format_args!(
                "INTERVAL DAY ({}) TO SECOND ({})",
                ids.precision, ids.fraction
            )
            .into(),
            ExaDataType::IntervalYearToMonth(iym) => {
                format_args!("INTERVAL YEAR ({}) TO MONTH", iym.precision).into()
            }
            // For HASHTYPE the database returns the size as doubled.
            ExaDataType::Hashtype(h) => {
                format_args!("{}({} BYTE)", self.as_ref(), h.size / 2).into()
            }
        }
    }
}

impl AsRef<str> for ExaDataType {
    fn as_ref(&self) -> &str {
        match self {
            ExaDataType::Null => Self::NULL,
            ExaDataType::Boolean => Self::BOOLEAN,
            ExaDataType::Char(_) => Self::CHAR,
            ExaDataType::Date => Self::DATE,
            ExaDataType::Decimal(_) => Self::DECIMAL,
            ExaDataType::Double => Self::DOUBLE,
            ExaDataType::Geometry(_) => Self::GEOMETRY,
            ExaDataType::IntervalDayToSecond(_) => Self::INTERVAL_DAY_TO_SECOND,
            ExaDataType::IntervalYearToMonth(_) => Self::INTERVAL_YEAR_TO_MONTH,
            ExaDataType::Timestamp => Self::TIMESTAMP,
            ExaDataType::TimestampWithLocalTimeZone => Self::TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            ExaDataType::Varchar(_) => Self::VARCHAR,
            ExaDataType::Hashtype(_) => Self::HASHTYPE,
        }
    }
}

#[derive(Debug, Clone)]
enum DataTypeName {
    Static(&'static str),
    Inline(ArrayString<30>),
}

impl AsRef<str> for DataTypeName {
    fn as_ref(&self) -> &str {
        match self {
            DataTypeName::Static(s) => s,
            DataTypeName::Inline(s) => s.as_str(),
        }
    }
}

impl Display for DataTypeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

impl From<&'static str> for DataTypeName {
    fn from(value: &'static str) -> Self {
        Self::Static(value)
    }
}

impl From<Arguments<'_>> for DataTypeName {
    fn from(value: Arguments<'_>) -> Self {
        Self::Inline(ArrayString::try_from(value).expect("inline data type name too large"))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StringLike {
    size: usize,
    character_set: Charset,
}

impl StringLike {
    pub const MAX_STR_LEN: usize = 2_000_000;

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
    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        matches!(
            ty,
            ExaDataType::Char(_)
                | ExaDataType::Varchar(_)
                | ExaDataType::Null
                | ExaDataType::Date
                | ExaDataType::Geometry(_)
                | ExaDataType::Hashtype(_)
                | ExaDataType::IntervalDayToSecond(_)
                | ExaDataType::IntervalYearToMonth(_)
                | ExaDataType::Timestamp
                | ExaDataType::TimestampWithLocalTimeZone
        )
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

    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::Decimal(d) => self >= d,
            ExaDataType::Double => self.scale > 0,
            ExaDataType::Null => true,
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

    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::Geometry(g) => self.srid == g.srid,
            ExaDataType::Varchar(_) | ExaDataType::Char(_) | ExaDataType::Null => true,
            _ => false,
        }
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
            precision: Self::MAX_PRECISION,
            fraction: Self::MAX_SUPPORTED_FRACTION,
        }
    }
}

impl PartialOrd for IntervalDayToSecond {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let precision_cmp = self.precision.partial_cmp(&other.precision);
        let fraction_cmp = self.fraction.partial_cmp(&other.fraction);

        match (precision_cmp, fraction_cmp) {
            (Some(Ordering::Equal), Some(Ordering::Equal)) => Some(Ordering::Equal),
            (Some(Ordering::Equal), Some(Ordering::Less))
            | (Some(Ordering::Less), Some(Ordering::Less))
            | (Some(Ordering::Less), Some(Ordering::Equal)) => Some(Ordering::Less),
            (Some(Ordering::Equal), Some(Ordering::Greater))
            | (Some(Ordering::Greater), Some(Ordering::Greater))
            | (Some(Ordering::Greater), Some(Ordering::Equal)) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

impl IntervalDayToSecond {
    /// The fraction has the weird behavior of shifting the milliseconds up
    /// value and mixing it with the seconds, minutes, hours or even the days
    /// when the value exceeds 3 (the max milliseconds digits limit).
    ///
    /// See: https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_dsinterval.htm?Highlight=fraction%20interval
    ///
    /// Therefore, we'll only be handling fractions smaller or equal to 3, as I don't
    /// even know how to handle values above that
    pub const MAX_SUPPORTED_FRACTION: u32 = 3;
    pub const MAX_PRECISION: u32 = 9;

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

    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::IntervalDayToSecond(i) => self >= i,
            ExaDataType::Varchar(_) | ExaDataType::Char(_) | ExaDataType::Null => true,
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
        Self {
            precision: Self::MAX_PRECISION,
        }
    }
}

impl IntervalYearToMonth {
    const MAX_PRECISION: u32 = 9;

    pub fn new(precision: u32) -> Self {
        Self { precision }
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::IntervalYearToMonth(i) => self >= i,
            ExaDataType::Varchar(_) | ExaDataType::Char(_) | ExaDataType::Null => true,
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

    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::Hashtype(h) => self.size >= h.size,
            ExaDataType::Varchar(_) | ExaDataType::Char(_) | ExaDataType::Null => true,
            _ => false,
        }
    }
}
