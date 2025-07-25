use chrono::{Months, TimeDelta};
use sqlx_core::error::BoxDynError;

use crate::types::{ExaIntervalDayToSecond, ExaIntervalYearToMonth};

mod date;
mod datetime;

impl TryFrom<Months> for ExaIntervalYearToMonth {
    type Error = BoxDynError;

    fn try_from(value: Months) -> Result<Self, Self::Error> {
        let num_months = value.as_u32();
        let years =
            i32::try_from(num_months / 12).expect("months into years cannot exceed i32::MAX");
        let months = u8::try_from(num_months % 12).expect("months remainder cannot exceed 12");

        Ok(Self { years, months })
    }
}

impl TryFrom<TimeDelta> for ExaIntervalDayToSecond {
    type Error = BoxDynError;

    fn try_from(value: TimeDelta) -> Result<Self, Self::Error> {
        let days = i32::try_from(value.num_days()).map_err(Box::new)?;
        let hours = u8::try_from(value.num_hours() % 24).expect("remainder hours cannot exceed 24");
        let minutes =
            u8::try_from(value.num_minutes() % 60).expect("remained minutes cannot exceed 60");
        let seconds =
            u8::try_from(value.num_seconds() % 60).expect("remainder seconds cannot exceed 60");
        let milliseconds = u16::try_from(value.num_milliseconds() % 1000)
            .expect("remainder milliseconds cannot exceed 1000");

        Ok(Self {
            days,
            hours,
            minutes,
            seconds,
            milliseconds,
        })
    }
}
