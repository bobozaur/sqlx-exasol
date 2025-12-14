use chrono::TimeDelta;
use serde::Deserialize;
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
    types::ExaHasArrayType,
    value::ExaValueRef,
};

impl Type<Exasol> for TimeDelta {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::IntervalDayToSecond {
            precision: ExaDataType::INTERVAL_DTS_MAX_PRECISION,
            fraction: ExaDataType::INTERVAL_DTS_MAX_FRACTION,
        }
        .into()
    }
}

impl ExaHasArrayType for TimeDelta {}

impl Encode<'_, Exasol> for TimeDelta {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(format_args!(
            "{} {}:{}:{}.{}",
            self.num_days(),
            self.num_hours().abs() % 24,
            self.num_minutes().abs() % 60,
            self.num_seconds().abs() % 60,
            self.num_milliseconds().abs() % 1000
        ))?;

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

impl<'r> Decode<'r, Exasol> for TimeDelta {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        let input = <&str>::deserialize(value.value).map_err(Box::new)?;
        let input_err_fn = || format!("could not parse {input} as INTERVAL DAY TO SECOND");

        let (days, rest) = input.split_once(' ').ok_or_else(input_err_fn)?;
        let (hours, rest) = rest.split_once(':').ok_or_else(input_err_fn)?;
        let (minutes, rest) = rest.split_once(':').ok_or_else(input_err_fn)?;
        let (seconds, millis) = rest.split_once('.').ok_or_else(input_err_fn)?;

        let days: i64 = days.parse().map_err(Box::new)?;
        let hours: i64 = hours.parse().map_err(Box::new)?;
        let minutes: i64 = minutes.parse().map_err(Box::new)?;
        let seconds: i64 = seconds.parse().map_err(Box::new)?;
        let millis: i64 = millis.parse().map_err(Box::new)?;
        let sign = if days.is_negative() { -1 } else { 1 };

        let duration = TimeDelta::days(days)
            + TimeDelta::hours(hours * sign)
            + TimeDelta::minutes(minutes * sign)
            + TimeDelta::seconds(seconds * sign)
            + TimeDelta::milliseconds(millis * sign);

        Ok(duration)
    }
}
