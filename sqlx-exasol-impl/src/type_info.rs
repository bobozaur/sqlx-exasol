use std::{
    cmp::Ordering,
    fmt::{Arguments, Display},
};

use arrayvec::ArrayString;
use serde::{Deserialize, Serialize};
use sqlx_core::type_info::TypeInfo;

/// Information about an Exasol data type and implementor of [`TypeInfo`].
// Note that the [`DataTypeName`] is automatically constructed from the provided [`ExaDataType`].
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(from = "ExaDataType")]
pub struct ExaTypeInfo {
    pub(crate) name: DataTypeName,
    pub(crate) data_type: ExaDataType,
}

/// Manually implemented because we only want to serialize the `data_type` field while also
/// flattening the structure.
///
/// On [`Deserialize`] we simply convert from the [`ExaDataType`] to this.
impl Serialize for ExaTypeInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.data_type.serialize(serializer)
    }
}

impl From<ExaDataType> for ExaTypeInfo {
    fn from(data_type: ExaDataType) -> Self {
        let name = data_type.full_name();
        Self { name, data_type }
    }
}

impl PartialEq for ExaTypeInfo {
    fn eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type
    }
}

impl Display for ExaTypeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl TypeInfo for ExaTypeInfo {
    fn is_null(&self) -> bool {
        matches!(self.data_type, ExaDataType::Null)
    }

    /// We're going against `sqlx` here, but knowing the full data type definition
    /// is actually very helpful when displaying error messages, so... ¯\_(ツ)_/¯.
    ///
    /// In fact, error messages seem to be the only place where this is being used,
    /// particularly when trying to decode a value but the data type provided by the
    /// database does not match/fit inside the Rust data type.
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// Checks compatibility with other data types.
    ///
    /// Returns true if this [`ExaTypeInfo`] instance is able to accommodate the `other` instance.
    fn type_compatible(&self, other: &Self) -> bool
    where
        Self: Sized,
    {
        self.data_type.compatible(&other.data_type)
    }
}

/// Datatype definitions enum, as Exasol sees them.
///
/// If you manually construct them, be aware that there is a [`DataTypeName`] automatically
/// constructed when converting to [`ExaTypeInfo`] and there are compatibility checks set in place.
///
/// In case of incompatibility, the definition is displayed for troubleshooting.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
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
    HashType(HashType),
}

impl ExaDataType {
    const NULL: &'static str = "NULL";
    const BOOLEAN: &'static str = "BOOLEAN";
    const CHAR: &'static str = "CHAR";
    const DATE: &'static str = "DATE";
    const DECIMAL: &'static str = "DECIMAL";
    const DOUBLE: &'static str = "DOUBLE PRECISION";
    const GEOMETRY: &'static str = "GEOMETRY";
    const INTERVAL_DAY_TO_SECOND: &'static str = "INTERVAL DAY TO SECOND";
    const INTERVAL_YEAR_TO_MONTH: &'static str = "INTERVAL YEAR TO MONTH";
    const TIMESTAMP: &'static str = "TIMESTAMP";
    const TIMESTAMP_WITH_LOCAL_TIME_ZONE: &'static str = "TIMESTAMP WITH LOCAL TIME ZONE";
    const VARCHAR: &'static str = "VARCHAR";
    const HASHTYPE: &'static str = "HASHTYPE";

    /// Returns `true` if this instance is compatible with the other one provided.
    ///
    /// Compatibility means that the [`self`] instance is bigger/able to accommodate the other
    /// instance.
    pub fn compatible(&self, other: &Self) -> bool {
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
            ExaDataType::HashType(_) => matches!(
                other,
                ExaDataType::HashType(_)
                    | ExaDataType::Varchar(_)
                    | ExaDataType::Char(_)
                    | ExaDataType::Null
            ),
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
                "INTERVAL DAY({}) TO SECOND({})",
                ids.precision, ids.fraction
            )
            .into(),
            ExaDataType::IntervalYearToMonth(iym) => {
                format_args!("INTERVAL YEAR({}) TO MONTH", iym.precision).into()
            }
            ExaDataType::HashType(_) => format_args!("{}", self.as_ref()).into(),
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
            ExaDataType::HashType(_) => Self::HASHTYPE,
        }
    }
}

/// A data type's name, composed from an instance of [`ExaDataType`]. For performance's sake, since
/// data type names are small, we either store them statically or as inlined strings.
///
/// *IMPORTANT*: Creating absurd [`ExaDataType`] can result in panics if the name exceeds the
/// inlined strings max capacity. Valid values always fit.
#[derive(Debug, Clone, Copy)]
pub enum DataTypeName {
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

/// Common inner type for string like data types, such as `VARCHAR` or `CHAR`.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StringLike {
    size: usize,
    character_set: Charset,
}

impl StringLike {
    pub const MAX_VARCHAR_LEN: usize = 2_000_000;
    pub const MAX_CHAR_LEN: usize = 2000;

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

    /// Strings are complex and ensuring one fits inside a database column would imply a lot of overhead.
    ///
    /// So just let the database do its thing and throw an error.
    #[allow(clippy::unused_self)]
    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        matches!(
            ty,
            ExaDataType::Char(_)
                | ExaDataType::Varchar(_)
                | ExaDataType::Null
                | ExaDataType::Date
                | ExaDataType::Geometry(_)
                | ExaDataType::HashType(_)
                | ExaDataType::IntervalDayToSecond(_)
                | ExaDataType::IntervalYearToMonth(_)
                | ExaDataType::Timestamp
                | ExaDataType::TimestampWithLocalTimeZone
        )
    }
}

/// Exasol supported character sets.
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

/// The `DECIMAL` data type.
#[derive(Debug, Copy, Clone, Deserialize, Serialize, PartialEq)]
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
    /// It's fine for this precision to "overflow".
    /// The database will simply reject values too large.
    pub const MAX_128BIT_PRECISION: u32 = 39;
    pub const MAX_PRECISION: u32 = 36;
    pub const MAX_SCALE: u32 = 35;

    pub fn new(precision: u32, scale: u32) -> Self {
        Self { precision, scale }
    }

    pub fn precision(self) -> u32 {
        self.precision
    }

    pub fn scale(self) -> u32 {
        self.scale
    }

    pub fn compatible(self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::Decimal(d) => self >= *d,
            ExaDataType::Double => self.scale > 0,
            ExaDataType::Null => true,
            _ => false,
        }
    }
}

#[rustfmt::skip] // just to skip rules formatting
/// The purpose of this is to be able to tell if some [`Decimal`] fits inside another [`Decimal`].
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

        #[allow(clippy::match_same_arms, reason = "better readability if split")]
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

/// The `GEOMETRY` data type.
#[derive(Debug, Copy, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Geometry {
    srid: u16,
}

impl Geometry {
    pub fn new(srid: u16) -> Self {
        Self { srid }
    }

    pub fn srid(self) -> u16 {
        self.srid
    }

    pub fn compatible(self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::Geometry(g) => self.srid == g.srid,
            ExaDataType::Varchar(_) | ExaDataType::Char(_) | ExaDataType::Null => true,
            _ => false,
        }
    }
}

/// The `INTERVAL DAY TO SECOND` data type.
#[derive(Debug, Copy, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IntervalDayToSecond {
    precision: u32,
    fraction: u32,
}

impl PartialOrd for IntervalDayToSecond {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let precision_cmp = self.precision.partial_cmp(&other.precision);
        let fraction_cmp = self.fraction.partial_cmp(&other.fraction);

        match (precision_cmp, fraction_cmp) {
            (Some(Ordering::Equal), Some(Ordering::Equal)) => Some(Ordering::Equal),
            (Some(Ordering::Equal | Ordering::Less), Some(Ordering::Less))
            | (Some(Ordering::Less), Some(Ordering::Equal)) => Some(Ordering::Less),
            (Some(Ordering::Equal | Ordering::Greater), Some(Ordering::Greater))
            | (Some(Ordering::Greater), Some(Ordering::Equal)) => Some(Ordering::Greater),
            _ => None,
        }
    }
}

impl IntervalDayToSecond {
    /// The fraction has the weird behavior of shifting the milliseconds up the value and mixing it
    /// with the seconds, minutes, hours or even the days when the value exceeds 3 (the max
    /// milliseconds digits limit).
    ///
    /// See: <`https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_dsinterval.htm?Highlight=fraction%20interval`>
    ///
    /// Therefore, we'll only be handling fractions smaller or equal to 3, as I don't even know how
    /// to handle values above that.
    pub const MAX_SUPPORTED_FRACTION: u32 = 3;
    pub const MAX_PRECISION: u32 = 9;

    pub fn new(precision: u32, fraction: u32) -> Self {
        Self {
            precision,
            fraction,
        }
    }

    pub fn precision(self) -> u32 {
        self.precision
    }

    pub fn fraction(self) -> u32 {
        self.fraction
    }

    pub fn compatible(self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::IntervalDayToSecond(i) => self >= *i,
            ExaDataType::Varchar(_) | ExaDataType::Char(_) | ExaDataType::Null => true,
            _ => false,
        }
    }
}

/// The `INTERVAL YEAR TO MONTH` data type.
#[derive(Debug, Copy, Clone, Deserialize, Serialize, PartialEq, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub struct IntervalYearToMonth {
    precision: u32,
}

impl IntervalYearToMonth {
    pub const MAX_PRECISION: u32 = 9;

    pub fn new(precision: u32) -> Self {
        Self { precision }
    }

    pub fn precision(self) -> u32 {
        self.precision
    }

    pub fn compatible(self, ty: &ExaDataType) -> bool {
        match ty {
            ExaDataType::IntervalYearToMonth(i) => self >= *i,
            ExaDataType::Varchar(_) | ExaDataType::Char(_) | ExaDataType::Null => true,
            _ => false,
        }
    }
}

/// The Exasol `HASHTYPE` data type.
///
/// NOTE: Exasol returns the size of the column string representation which depends on the
/// `HASHTYPE_FORMAT` database parameter. So a UUID could have a size of 32 for HEX, 36 for
/// UUID, 22 for BASE64, etc.
///
/// That makes it impossible to compare a type's predefined size and the size returned for a
/// column to check for compatibility.
#[derive(Debug, Copy, Clone, Deserialize, Serialize, PartialEq, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub struct HashType {}

/// Mainly adding these so that we ensure the inlined type names won't panic when created with their
/// max values.
///
/// If the max values work, the lower ones inherently will too.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_name() {
        let data_type = ExaDataType::Null;
        assert_eq!(data_type.full_name().as_ref(), "NULL");
    }

    #[test]
    fn test_boolean_name() {
        let data_type = ExaDataType::Boolean;
        assert_eq!(data_type.full_name().as_ref(), "BOOLEAN");
    }

    #[test]
    fn test_max_char_name() {
        let string_like = StringLike::new(StringLike::MAX_CHAR_LEN, Charset::Ascii);
        let data_type = ExaDataType::Char(string_like);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!("CHAR({}) ASCII", StringLike::MAX_CHAR_LEN)
        );
    }

    #[test]
    fn test_date_name() {
        let data_type = ExaDataType::Date;
        assert_eq!(data_type.full_name().as_ref(), "DATE");
    }

    #[test]
    fn test_max_decimal_name() {
        let decimal = Decimal::new(Decimal::MAX_PRECISION, Decimal::MAX_SCALE);
        let data_type = ExaDataType::Decimal(decimal);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!(
                "DECIMAL({}, {})",
                Decimal::MAX_PRECISION,
                Decimal::MAX_SCALE
            )
        );
    }

    #[test]
    fn test_double_name() {
        let data_type = ExaDataType::Double;
        assert_eq!(data_type.full_name().as_ref(), "DOUBLE PRECISION");
    }

    #[test]
    fn test_max_geometry_name() {
        let geometry = Geometry::new(u16::MAX);
        let data_type = ExaDataType::Geometry(geometry);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!("GEOMETRY({})", u16::MAX)
        );
    }

    #[test]
    fn test_max_interval_day_name() {
        let ids = IntervalDayToSecond::new(
            IntervalDayToSecond::MAX_PRECISION,
            IntervalDayToSecond::MAX_SUPPORTED_FRACTION,
        );
        let data_type = ExaDataType::IntervalDayToSecond(ids);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!(
                "INTERVAL DAY({}) TO SECOND({})",
                IntervalDayToSecond::MAX_PRECISION,
                IntervalDayToSecond::MAX_SUPPORTED_FRACTION
            )
        );
    }

    #[test]
    fn test_max_interval_year_name() {
        let iym = IntervalYearToMonth::new(IntervalYearToMonth::MAX_PRECISION);
        let data_type = ExaDataType::IntervalYearToMonth(iym);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!(
                "INTERVAL YEAR({}) TO MONTH",
                IntervalYearToMonth::MAX_PRECISION,
            )
        );
    }

    #[test]
    fn test_timestamp_name() {
        let data_type = ExaDataType::Timestamp;
        assert_eq!(data_type.full_name().as_ref(), "TIMESTAMP");
    }

    #[test]
    fn test_timestamp_with_tz_name() {
        let data_type = ExaDataType::TimestampWithLocalTimeZone;
        assert_eq!(
            data_type.full_name().as_ref(),
            "TIMESTAMP WITH LOCAL TIME ZONE"
        );
    }

    #[test]
    fn test_max_varchar_name() {
        let string_like = StringLike::new(StringLike::MAX_VARCHAR_LEN, Charset::Ascii);
        let data_type = ExaDataType::Varchar(string_like);
        assert_eq!(
            data_type.full_name().as_ref(),
            format!("VARCHAR({}) ASCII", StringLike::MAX_VARCHAR_LEN)
        );
    }
}
