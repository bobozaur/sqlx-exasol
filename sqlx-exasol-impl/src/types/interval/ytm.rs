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
    type_info::{ExaDataType, ExaTypeInfo, IntervalYearToMonth},
    value::ExaValueRef,
};

/// A duration interval as a representation of the `INTERVAL YEAR TO MONTH` datatype.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub struct ExaIntervalYearToMonth {
    pub years: i32,
    pub months: u8,
}

impl Type<Exasol> for ExaIntervalYearToMonth {
    fn type_info() -> ExaTypeInfo {
        let iym = IntervalYearToMonth::new(IntervalYearToMonth::MAX_PRECISION);
        ExaDataType::IntervalYearToMonth(iym).into()
    }
}

impl Encode<'_, Exasol> for ExaIntervalYearToMonth {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // 1 quote + 1 sign + max year precision + 1 dash + 2 months + 1 quote
        2 + IntervalYearToMonth::MAX_PRECISION as usize + 4
    }
}

impl<'r> Decode<'r, Exasol> for ExaIntervalYearToMonth {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        Self::deserialize(value.value).map_err(From::from)
    }
}

impl Display for ExaIntervalYearToMonth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.years, self.months)
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
                let years = years.parse::<i32>().map_err(de::Error::custom)?;
                let months = months.parse::<u8>().map_err(de::Error::custom)?;

                Ok(ExaIntervalYearToMonth { years, months })
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}
