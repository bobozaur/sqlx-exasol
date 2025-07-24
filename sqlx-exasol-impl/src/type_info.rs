use std::fmt::{Arguments, Display};

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
        false
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
    /// The BOOLEAN data type.
    Boolean,
    /// The CHAR data type.
    Char(StringLike),
    /// The DATE data type.
    Date,
    /// The DECIMAL data type.
    Decimal(Decimal),
    /// The DOUBLE data type.
    Double,
    /// The `GEOMETRY` data type.
    #[serde(rename_all = "camelCase")]
    Geometry { srid: u16 },
    /// The `INTERVAL DAY TO SECOND` data type.
    #[serde(rename = "INTERVAL DAY TO SECOND")]
    #[serde(rename_all = "camelCase")]
    IntervalDayToSecond { precision: u32, fraction: u32 },
    /// The `INTERVAL YEAR TO MONTH` data type.
    #[serde(rename = "INTERVAL YEAR TO MONTH")]
    #[serde(rename_all = "camelCase")]
    IntervalYearToMonth { precision: u32 },
    /// The TIMESTAMP data type.
    Timestamp,
    /// The TIMESTAMP WITH LOCAL TIME ZONE data type.
    #[serde(rename = "TIMESTAMP WITH LOCAL TIME ZONE")]
    TimestampWithLocalTimeZone,
    /// The VARCHAR data type.
    Varchar(StringLike),
    /// The Exasol `HASHTYPE` data type.
    ///
    /// NOTE: Exasol returns the size of the column string representation which depends on the
    /// `HASHTYPE_FORMAT` database parameter. So a UUID could have a size of 32 for HEX, 36 for
    /// UUID, 22 for BASE64, etc.
    ///
    /// That makes it impossible to compare a type's predefined size and the size returned for a
    /// column to check for compatibility.
    HashType,
}

impl ExaDataType {
    // Data type names
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

    // Datatype constants
    pub(crate) const INTERVAL_YTM_MAX_PRECISION: u32 = 9;
    /// Accuracy is limited to milliseconds, see: <https://docs.exasol.com/db/latest/sql_references/data_types/datatypedetails.htm#Interval>.
    ///
    /// The fraction has the weird behavior of shifting the milliseconds up the value and mixing it
    /// with the seconds, minutes, hours or even the days when the value exceeds 3 (the max
    /// milliseconds digits limit) even though the maximum value is 9.
    ///
    /// See: <https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/to_dsinterval.htm?Highlight=fraction%20interval>
    ///
    /// Therefore, we'll only be handling fractions smaller or equal to 3.
    pub(crate) const INTERVAL_DTS_MAX_FRACTION: u32 = 3;
    pub(crate) const INTERVAL_DTS_MAX_PRECISION: u32 = 9;

    /// Returns `true` if this instance is compatible with the other one provided.
    ///
    /// Compatibility means that the [`self`] instance is bigger/able to accommodate the other
    /// instance.
    pub fn compatible(&self, other: &Self) -> bool {
        match self {
            ExaDataType::Boolean => matches!(other, ExaDataType::Boolean),
            ExaDataType::Char(s) | ExaDataType::Varchar(s) => s.compatible(other),
            ExaDataType::Date => matches!(other, ExaDataType::Date),
            ExaDataType::Decimal(d) => d.compatible(other),
            ExaDataType::Double => matches!(other, ExaDataType::Double),
            ExaDataType::Geometry { .. } => matches!(
                other,
                ExaDataType::Geometry { .. }
                    | ExaDataType::Char { .. }
                    | ExaDataType::Varchar { .. }
            ),
            ExaDataType::IntervalDayToSecond { .. } => {
                matches!(other, ExaDataType::IntervalDayToSecond { .. })
            }
            ExaDataType::IntervalYearToMonth { .. } => {
                matches!(other, ExaDataType::IntervalYearToMonth { .. })
            }
            ExaDataType::Timestamp => matches!(other, ExaDataType::Timestamp),
            ExaDataType::TimestampWithLocalTimeZone => {
                matches!(other, ExaDataType::TimestampWithLocalTimeZone)
            }
            ExaDataType::HashType => matches!(
                other,
                ExaDataType::HashType | ExaDataType::Varchar { .. } | ExaDataType::Char { .. }
            ),
        }
    }

    fn full_name(&self) -> DataTypeName {
        match self {
            ExaDataType::Boolean => Self::BOOLEAN.into(),
            ExaDataType::Date => Self::DATE.into(),
            ExaDataType::Double => Self::DOUBLE.into(),
            ExaDataType::Timestamp => Self::TIMESTAMP.into(),
            ExaDataType::TimestampWithLocalTimeZone => Self::TIMESTAMP_WITH_LOCAL_TIME_ZONE.into(),
            ExaDataType::Char(s) | ExaDataType::Varchar(s) => match s.character_set {
                Some(c) => format_args!("{}({}) {c}", self.as_ref(), s.size).into(),
                None => format_args!("{}({})", self.as_ref(), s.size).into(),
            },
            ExaDataType::Decimal(d) => match d.precision {
                Some(p) => format_args!("{}({}, {})", self.as_ref(), p, d.scale).into(),
                None => format_args!("{}(*, {})", self.as_ref(), d.scale).into(),
            },
            ExaDataType::Geometry { srid } => format_args!("{}({srid})", self.as_ref()).into(),
            ExaDataType::IntervalDayToSecond {
                precision,
                fraction,
            } => format_args!("INTERVAL DAY({precision}) TO SECOND({fraction})").into(),
            ExaDataType::IntervalYearToMonth { precision } => {
                format_args!("INTERVAL YEAR({precision}) TO MONTH").into()
            }
            ExaDataType::HashType => format_args!("{}", self.as_ref()).into(),
        }
    }
}

impl AsRef<str> for ExaDataType {
    fn as_ref(&self) -> &str {
        match self {
            ExaDataType::Boolean => Self::BOOLEAN,
            ExaDataType::Char { .. } => Self::CHAR,
            ExaDataType::Date => Self::DATE,
            ExaDataType::Decimal(_) => Self::DECIMAL,
            ExaDataType::Double => Self::DOUBLE,
            ExaDataType::Geometry { .. } => Self::GEOMETRY,
            ExaDataType::IntervalDayToSecond { .. } => Self::INTERVAL_DAY_TO_SECOND,
            ExaDataType::IntervalYearToMonth { .. } => Self::INTERVAL_YEAR_TO_MONTH,
            ExaDataType::Timestamp => Self::TIMESTAMP,
            ExaDataType::TimestampWithLocalTimeZone => Self::TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            ExaDataType::Varchar { .. } => Self::VARCHAR,
            ExaDataType::HashType => Self::HASHTYPE,
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

/// The `DECIMAL` data type.
#[derive(Debug, Copy, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Decimal {
    pub(crate) precision: Option<u8>,
    pub(crate) scale: u8,
}

impl Decimal {
    /// Max precision values for signed integers.
    pub(crate) const MAX_8BIT_PRECISION: u8 = 3;
    pub(crate) const MAX_16BIT_PRECISION: u8 = 5;
    pub(crate) const MAX_32BIT_PRECISION: u8 = 10;
    pub(crate) const MAX_64BIT_PRECISION: u8 = 20;

    /// Max supported values.
    pub(crate) const MAX_PRECISION: u8 = 36;
    #[allow(dead_code)]
    pub(crate) const MAX_SCALE: u8 = 36;

    /// The purpose of this is to be able to tell if some [`Decimal`] fits inside another
    /// [`Decimal`].
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
    /// This boils down to:
    /// `a.scale >= b.scale AND (a.precision - a.scale) >= (b.precision - b.scale)`
    ///
    /// However, decimal Rust types require special handling because they can hold virtually any
    /// decoded value. Therefore, an absent precision means that the comparison must be skipped.
    #[rustfmt::skip] // just to skip rules formatting
    fn compatible(self, ty: &ExaDataType) -> bool {
        let (precision, scale) = match ty {
            // Short-circuit if we are encoding a Rust decimal type as they have arbitrary precision.
            ExaDataType::Decimal(Decimal { precision: None, .. })  => return true,
            ExaDataType::Decimal(Decimal { precision: Some(precision), scale }) => (*precision, *scale),
            _ => return false,
        };

        // If we're decoding to a Rust decimal type then we can accept any DECIMAL precision.
        let self_diff = self.precision.map_or(Decimal::MAX_PRECISION, |p| p - self.scale);
        let other_diff = precision - scale;

        self.scale >= scale && self_diff >= other_diff
    }
}

/// Common inner type for string like data types, such as `VARCHAR` or `CHAR`.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StringLike {
    pub(crate) size: usize,
    /// The absence of a charset
    pub(crate) character_set: Option<Charset>,
}

impl StringLike {
    pub(crate) const MAX_VARCHAR_LEN: usize = 2_000_000;
    #[allow(dead_code)]
    pub(crate) const MAX_CHAR_LEN: usize = 2000;

    /// We don't care much about the size but we do care about the charset.
    ///
    /// If the column charset is ASCII, UTF-8 strings like Rust's might not be encoded and stored as
    /// expected. Conversely, if the column charset is UTF-8, an ASCII only type not be the best
    /// type to decode it to.
    ///
    /// The absence of a charset means that the value can go into any (VAR)CHAR column.
    pub fn compatible(&self, ty: &ExaDataType) -> bool {
        match ty {
            // Allow decoding GEOMETRY / HASHTYPE data to strings
            ExaDataType::Geometry { .. } | ExaDataType::HashType
                if self.character_set == Some(Charset::Utf8) =>
            {
                true
            }
            ExaDataType::Char(other) | ExaDataType::Varchar(other) => matches!(
                (self.character_set, other.character_set),
                (Some(Charset::Utf8), _) | (Some(Charset::Ascii), Some(Charset::Ascii) | None)
            ),
            _ => false,
        }
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

/// Mainly adding these so that we ensure the inlined type names won't panic when created with their
/// max values.
///
/// If the max values work, the lower ones inherently will too.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_name() {
        let data_type = ExaDataType::Boolean;
        assert_eq!(data_type.full_name().as_ref(), "BOOLEAN");
    }

    #[test]
    fn test_max_char_name() {
        let data_type = ExaDataType::Char(StringLike {
            size: StringLike::MAX_CHAR_LEN,
            character_set: Some(Charset::Ascii),
        });
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
        let decimal = Decimal {
            precision: Some(Decimal::MAX_PRECISION),
            scale: Decimal::MAX_SCALE,
        };
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
        let data_type = ExaDataType::Geometry { srid: u16::MAX };
        assert_eq!(
            data_type.full_name().as_ref(),
            format!("GEOMETRY({})", u16::MAX)
        );
    }

    #[test]
    fn test_max_interval_day_name() {
        let data_type = ExaDataType::IntervalDayToSecond {
            precision: ExaDataType::INTERVAL_DTS_MAX_PRECISION,
            fraction: ExaDataType::INTERVAL_DTS_MAX_FRACTION,
        };
        assert_eq!(
            data_type.full_name().as_ref(),
            format!(
                "INTERVAL DAY({}) TO SECOND({})",
                ExaDataType::INTERVAL_DTS_MAX_PRECISION,
                ExaDataType::INTERVAL_DTS_MAX_FRACTION
            )
        );
    }

    #[test]
    fn test_max_interval_year_name() {
        let data_type = ExaDataType::IntervalYearToMonth {
            precision: ExaDataType::INTERVAL_YTM_MAX_PRECISION,
        };
        assert_eq!(
            data_type.full_name().as_ref(),
            format!(
                "INTERVAL YEAR({}) TO MONTH",
                ExaDataType::INTERVAL_YTM_MAX_PRECISION,
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
        let data_type = ExaDataType::Char(StringLike {
            size: StringLike::MAX_VARCHAR_LEN,
            character_set: Some(Charset::Ascii),
        });
        assert_eq!(
            data_type.full_name().as_ref(),
            format!("VARCHAR({}) ASCII", StringLike::MAX_VARCHAR_LEN)
        );
    }
}
