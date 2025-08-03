use std::fmt::{self, Display};

use serde::{de, Deserialize, Deserializer, Serialize};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{ExaDataType, ExaTypeInfo},
    value::ExaValueRef,
};

/// A duration interval as a representation of the `INTERVAL YEAR TO MONTH` datatype.
///
/// The duration is expressed in months.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub struct ExaIntervalYearToMonth(pub i64);

impl Type<Exasol> for ExaIntervalYearToMonth {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::IntervalYearToMonth {
            precision: ExaDataType::INTERVAL_YTM_MAX_PRECISION,
        }
        .into()
    }
}

impl Encode<'_, Exasol> for ExaIntervalYearToMonth {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // 1 quote + 1 sign + max year precision + 1 dash + 2 months + 1 quote
        2 + ExaDataType::INTERVAL_YTM_MAX_PRECISION as usize + 4
    }
}

impl<'r> Decode<'r, Exasol> for ExaIntervalYearToMonth {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        Self::deserialize(value.value).map_err(From::from)
    }
}

impl Display for ExaIntervalYearToMonth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let years = self.0 / 12;
        let months = (self.0 % 12).abs();
        let plus = if years.is_negative() { "" } else { "+" };
        write!(f, "{plus}{years}-{months}")
    }
}

impl Serialize for ExaIntervalYearToMonth {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        format_args!("{self}").serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExaIntervalYearToMonth {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = ExaIntervalYearToMonth;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "INTERVAL YEAR TO MONTH in the format [year]-[month]")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                let input_err_fn = || {
                    let msg = format!("could not parse {value} as INTERVAL YEAR TO MONTH");
                    de::Error::custom(msg)
                };

                let (years, months) = value.rsplit_once('-').ok_or_else(input_err_fn)?;
                let years = years.parse::<i64>().map_err(de::Error::custom)?;
                let months = months.parse::<i64>().map_err(de::Error::custom)?;

                let sign = if years.is_negative() { -1 } else { 1 };
                let total = years * 12 + months * sign;

                Ok(ExaIntervalYearToMonth(total))
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}
