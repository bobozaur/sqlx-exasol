use sqlx_core::error::BoxDynError;
use time::Duration;

use crate::types::ExaIntervalDayToSecond;

mod date;
mod datetime;

impl TryFrom<Duration> for ExaIntervalDayToSecond {
    type Error = BoxDynError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        let days = i32::try_from(value.whole_days()).map_err(Box::new)?;
        let hours =
            u8::try_from(value.whole_hours() % 24).expect("remainder hours cannot exceed 24");
        let minutes =
            u8::try_from(value.whole_minutes() % 60).expect("remainder minutes cannot exceed 60");
        let seconds =
            u8::try_from(value.whole_seconds() % 60).expect("remainder seconds cannot exceed 60");
        let milliseconds = u16::try_from(value.whole_milliseconds() % 1000)
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
