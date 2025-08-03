use serde::Deserialize;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};
use time::Duration;

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{ExaDataType, ExaTypeInfo},
    value::ExaValueRef,
};

impl Type<Exasol> for Duration {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::IntervalDayToSecond {
            precision: ExaDataType::INTERVAL_DTS_MAX_PRECISION,
            fraction: ExaDataType::INTERVAL_DTS_MAX_FRACTION,
        }
        .into()
    }
}

impl Encode<'_, Exasol> for Duration {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(format_args!(
            "{} {}:{}:{}.{}",
            self.whole_days(),
            self.whole_hours().abs() % 24,
            self.whole_minutes().abs() % 60,
            self.whole_seconds().abs() % 60,
            self.whole_milliseconds().abs() % 1000
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

impl<'r> Decode<'r, Exasol> for Duration {
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

        let duration = Duration::days(days)
            + Duration::hours(hours * sign)
            + Duration::minutes(minutes * sign)
            + Duration::seconds(seconds * sign)
            + Duration::milliseconds(millis * sign);

        Ok(duration)
    }
}
