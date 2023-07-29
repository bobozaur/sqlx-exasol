use std::{
    borrow::Cow,
    ops::{Add, Sub},
};

use chrono::{DateTime, Datelike, Local, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{
    database::Exasol,
    type_info::{ExaTypeInfo, IntervalDayToSecond, IntervalYearToMonth},
    value::ExaValueRef,
};

use super::impl_encode_decode;

impl Type<Exasol> for DateTime<Utc> {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Timestamp
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Timestamp)
    }
}

impl Encode<'_, Exasol> for DateTime<Utc> {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        Encode::<Exasol>::encode(self.naive_utc(), buf)
    }
}

impl<'r> Decode<'r, Exasol> for DateTime<Utc> {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        let naive: NaiveDateTime = Decode::<Exasol>::decode(value)?;
        Ok(DateTime::from_utc(naive, Utc))
    }
}

impl Type<Exasol> for DateTime<Local> {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::TimestampWithLocalTimeZone
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::TimestampWithLocalTimeZone)
    }
}

impl Encode<'_, Exasol> for DateTime<Local> {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        Encode::<Exasol>::encode(self.naive_utc(), buf)
    }
}

impl<'r> Decode<'r, Exasol> for DateTime<Local> {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        Self::deserialize(value.value).map_err(From::from)
    }
}

impl Type<Exasol> for NaiveDate {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Date
    }
}

impl Type<Exasol> for NaiveDateTime {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Timestamp
    }
}

impl Type<Exasol> for chrono::Duration {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::IntervalDayToSecond(IntervalDayToSecond::new(2, 3))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::IntervalDayToSecond(_))
    }
}

impl Encode<'_, Exasol> for chrono::Duration {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        buf.push([json!(format_args!(
            "{} {}:{}:{}.{}",
            self.num_days(),
            self.num_hours(),
            self.num_minutes(),
            self.num_seconds(),
            self.num_milliseconds()
        ))]);
        IsNull::No
    }
}

impl<'r> Decode<'r, Exasol> for chrono::Duration {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        let input = Cow::<str>::deserialize(value.value).map_err(Box::new)?;

        let (days, rest) = input
            .split_once(' ')
            .ok_or_else(|| format!("could not parse {input} as INTERVAL DAY TO SECOND"))?;
        let (hours, rest) = rest
            .split_once(':')
            .ok_or_else(|| format!("could not parse {input} as INTERVAL DAY TO SECOND"))?;
        let (minutes, rest) = rest
            .split_once(':')
            .ok_or_else(|| format!("could not parse {input} as INTERVAL DAY TO SECOND"))?;
        let (seconds, millis) = rest
            .split_once('.')
            .ok_or_else(|| format!("could not parse {input} as INTERVAL DAY TO SECOND"))?;

        let days = days.parse().map_err(Box::new)?;
        let hours = hours.parse().map_err(Box::new)?;
        let minutes = minutes.parse().map_err(Box::new)?;
        let seconds = seconds.parse().map_err(Box::new)?;
        let millis = millis.parse().map_err(Box::new)?;

        let duration = chrono::Duration::days(days)
            + chrono::Duration::hours(hours)
            + chrono::Duration::minutes(minutes)
            + chrono::Duration::seconds(seconds)
            + chrono::Duration::milliseconds(millis);

        Ok(duration)
    }
}

impl_encode_decode!(NaiveDate);
impl_encode_decode!(NaiveDateTime);

/// A duration in calendar months
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub struct Months(i32);

impl Months {
    /// Construct a new `Months` from a number of months
    pub const fn new(num: i32) -> Self {
        Self(num)
    }

    pub fn num_months(&self) -> i32 {
        self.0
    }
}

impl<Tz: TimeZone> Add<Months> for DateTime<Tz> {
    type Output = DateTime<Tz>;

    fn add(self, rhs: Months) -> Self::Output {
        let months = chrono::Months::new(rhs.0 as u32);

        if rhs.0.is_negative() {
            self.checked_sub_months(months).unwrap()
        } else {
            self.checked_add_months(months).unwrap()
        }
    }
}

impl Add<Months> for NaiveDate {
    type Output = NaiveDate;

    fn add(self, rhs: Months) -> Self::Output {
        let months = chrono::Months::new(rhs.0 as u32);

        if rhs.0.is_negative() {
            self.checked_sub_months(months).unwrap()
        } else {
            self.checked_add_months(months).unwrap()
        }
    }
}

impl Add<Months> for NaiveDateTime {
    type Output = NaiveDateTime;

    fn add(self, rhs: Months) -> Self::Output {
        let months = chrono::Months::new(rhs.0 as u32);

        if rhs.0.is_negative() {
            self.checked_sub_months(months).unwrap()
        } else {
            self.checked_add_months(months).unwrap()
        }
    }
}

impl<Tz: TimeZone> Sub<Months> for DateTime<Tz> {
    type Output = DateTime<Tz>;

    fn sub(self, rhs: Months) -> Self::Output {
        let months = chrono::Months::new(rhs.0 as u32);

        if rhs.0.is_negative() {
            self.checked_add_months(months).unwrap()
        } else {
            self.checked_sub_months(months).unwrap()
        }
    }
}

impl Sub<Months> for NaiveDate {
    type Output = NaiveDate;

    fn sub(self, rhs: Months) -> Self::Output {
        let months = chrono::Months::new(rhs.0 as u32);

        if rhs.0.is_negative() {
            self.checked_add_months(months).unwrap()
        } else {
            self.checked_sub_months(months).unwrap()
        }
    }
}

impl Sub<Months> for NaiveDateTime {
    type Output = NaiveDateTime;

    fn sub(self, rhs: Months) -> Self::Output {
        let months = chrono::Months::new(rhs.0 as u32);

        if rhs.0.is_negative() {
            self.checked_add_months(months).unwrap()
        } else {
            self.checked_sub_months(months).unwrap()
        }
    }
}

impl Type<Exasol> for Months {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::IntervalYearToMonth(IntervalYearToMonth::new(2))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::IntervalYearToMonth(_))
    }
}

impl Encode<'_, Exasol> for Months {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        let date = NaiveDate::default();
        let date_with_months = NaiveDate::default() + *self;
        let years = date_with_months.year() - date.year();
        let months = date_with_months.month() - date.month();
        buf.push([json!(format_args!("{}-{}", years, months))]);
        IsNull::No
    }
}

impl<'r> Decode<'r, Exasol> for Months {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        let input = Cow::<str>::deserialize(value.value).map_err(Box::new)?;
        let (years, months) = input
            .rsplit_once('-')
            .ok_or_else(|| format!("could not parse {input} as INTERVAL YEAR TO MONTH"))?;

        let years = years.parse::<i32>().map_err(Box::new)?;
        let months = months.parse::<i32>().map_err(Box::new)?;

        let total_months = match years.is_negative() {
            true => years * 12 - months,
            false => years * 12 + months,
        };

        Ok(Months::new(total_months))
    }
}
