use std::{
    fmt::{self, Display},
    time::Duration,
};

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

/// A duration interval as a representation of the `INTERVAL DAY TO SECOND` datatype.
///
/// Note that the sign of the whole type is held by the `days` field.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub struct ExaIntervalDayToSecond {
    pub days: i32,
    pub hours: u8,
    pub minutes: u8,
    pub seconds: u8,
    pub milliseconds: u16,
}

impl Type<Exasol> for ExaIntervalDayToSecond {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::IntervalDayToSecond {
            precision: ExaDataType::INTERVAL_DTS_MAX_PRECISION,
            fraction: ExaDataType::INTERVAL_DTS_MAX_FRACTION,
        }
        .into()
    }
}

impl Encode<'_, Exasol> for ExaIntervalDayToSecond {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // 1 quote + 1 sign + max days precision +
        // 1 space + 2 hours + 1 column + 2 minutes + 1 column + 2 seconds +
        // 1 dot + max milliseconds fraction +
        // 1 quote
        2 + ExaDataType::INTERVAL_DTS_MAX_PRECISION as usize
            + 10
            + ExaDataType::INTERVAL_DTS_MAX_FRACTION as usize
            + 1
    }
}

impl<'r> Decode<'r, Exasol> for ExaIntervalDayToSecond {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        Self::deserialize(value.value).map_err(From::from)
    }
}

impl Display for ExaIntervalDayToSecond {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let plus = if self.days.is_negative() { "" } else { "+" };
        write!(
            f,
            "{}{} {}:{}:{}.{}",
            plus, self.days, self.hours, self.minutes, self.seconds, self.milliseconds
        )
    }
}

impl Serialize for ExaIntervalDayToSecond {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        format_args!("{self}").serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExaIntervalDayToSecond {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = ExaIntervalDayToSecond;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "INTERVAL YEAR TO MONTH in the format [year]-[month]")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                let input_err_fn = || {
                    let msg = format!("could not parse {value} as INTERVAL DAY TO SECOND");
                    de::Error::custom(msg)
                };

                let (days, rest) = value.split_once(' ').ok_or_else(input_err_fn)?;
                let (hours, rest) = rest.split_once(':').ok_or_else(input_err_fn)?;
                let (minutes, rest) = rest.split_once(':').ok_or_else(input_err_fn)?;
                let (seconds, millis) = rest.split_once('.').ok_or_else(input_err_fn)?;

                let days: i32 = days.parse().map_err(de::Error::custom)?;
                let hours: u8 = hours.parse().map_err(de::Error::custom)?;
                let minutes: u8 = minutes.parse().map_err(de::Error::custom)?;
                let seconds: u8 = seconds.parse().map_err(de::Error::custom)?;
                let milliseconds: u16 = millis.parse().map_err(de::Error::custom)?;

                Ok(ExaIntervalDayToSecond {
                    days,
                    hours,
                    minutes,
                    seconds,
                    milliseconds,
                })
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

impl TryFrom<Duration> for ExaIntervalDayToSecond {
    type Error = BoxDynError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        let num_seconds = value.as_secs();

        let milliseconds = u16::try_from(value.as_millis() % 1000)
            .expect("remainder milliseconds cannot exceed 1000");
        let seconds = u8::try_from(num_seconds % 60).expect("remainder seconds cannot exceed 60");
        let minutes =
            u8::try_from(num_seconds / 60 % 60).expect("remainder minutes cannot exceed 60");
        let hours =
            u8::try_from(num_seconds / 60 / 60 % 24).expect("remainder hours cannot exceed 24");
        let days = i32::try_from(num_seconds / 60 / 60 / 24).map_err(Box::new)?;

        Ok(Self {
            days,
            hours,
            minutes,
            seconds,
            milliseconds,
        })
    }
}
