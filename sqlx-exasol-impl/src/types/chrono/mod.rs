use chrono::Months;
use sqlx_core::error::BoxDynError;

use crate::types::ExaIntervalYearToMonth;

mod date;
mod datetime;
mod timedelta;

pub use chrono::TimeDelta;

impl TryFrom<Months> for ExaIntervalYearToMonth {
    type Error = BoxDynError;

    fn try_from(value: Months) -> Result<Self, Self::Error> {
        let num_months = value.as_u32().into();
        Ok(Self(num_months))
    }
}
