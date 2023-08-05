use std::{
    borrow::Cow,
    cmp,
    ops::{Add, Sub},
};

use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeZone, Utc};
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
    type_info::{ExaTypeInfo, IntervalDayToSecond, IntervalYearToMonth},
    value::ExaValueRef,
};

use super::ExaParameter;

impl Type<Exasol> for DateTime<Utc> {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Timestamp
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Timestamp)
    }
}

impl Encode<'_, Exasol> for DateTime<Utc> {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        Encode::<Exasol>::encode(self.naive_utc(), buf)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaTypeInfo::Timestamp)
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
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        Encode::<Exasol>::encode(self.naive_utc(), buf)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaTypeInfo::TimestampWithLocalTimeZone)
    }
}

impl<'r> Decode<'r, Exasol> for DateTime<Local> {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        Self::deserialize(value.value).map_err(From::from)
    }
}

impl Type<Exasol> for chrono::Duration {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::IntervalDayToSecond(Default::default())
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::IntervalDayToSecond(_))
    }
}

impl Encode<'_, Exasol> for chrono::Duration {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(format_args!(
            "{} {}:{}:{}.{}",
            self.num_days(),
            self.num_hours(),
            self.num_minutes(),
            self.num_seconds(),
            self.num_milliseconds()
        ));

        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let precision = self
            .num_days()
            .unsigned_abs()
            .checked_ilog10()
            .unwrap_or_default()
            + 1;
        let precision = cmp::min(precision, 9);

        let fraction = self
            .num_milliseconds()
            .unsigned_abs()
            .checked_ilog10()
            .map(|v| v + 1)
            .unwrap_or_default();

        let fraction = cmp::min(fraction, 9);

        Some(ExaTypeInfo::IntervalDayToSecond(IntervalDayToSecond::new(
            precision, fraction,
        )))
    }
}

impl<'r> Decode<'r, Exasol> for chrono::Duration {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        let input = Cow::<str>::deserialize(value.value).map_err(Box::new)?;
        let input_err_fn = || format!("could not parse {input} as INTERVAL DAY TO SECOND");

        let (days, rest) = input.split_once(' ').ok_or_else(input_err_fn)?;
        let (hours, rest) = rest.split_once(':').ok_or_else(input_err_fn)?;
        let (minutes, rest) = rest.split_once(':').ok_or_else(input_err_fn)?;
        let (seconds, millis) = rest.split_once('.').ok_or_else(input_err_fn)?;

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

impl ExaParameter for NaiveDate {}

impl Type<Exasol> for NaiveDate {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Date
    }
}

impl Encode<'_, Exasol> for NaiveDate {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaTypeInfo::Date)
    }
}

impl Decode<'_, Exasol> for NaiveDate {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}

impl ExaParameter for NaiveDateTime {}

impl Type<Exasol> for NaiveDateTime {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Timestamp
    }
}

impl Encode<'_, Exasol> for NaiveDateTime {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaTypeInfo::Timestamp)
    }
}

impl Decode<'_, Exasol> for NaiveDateTime {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}

/// A duration in calendar months, analog to [`chrono::Months`].
/// Unlike [`chrono::Months`], this type can represent a negative duration.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub struct Months(i32);

impl Months {
    pub const fn new(num: i32) -> Self {
        Self(num)
    }

    pub fn num_months(&self) -> i32 {
        self.0
    }
}

impl Type<Exasol> for Months {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::IntervalYearToMonth(Default::default())
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::IntervalYearToMonth(_))
    }
}

impl Encode<'_, Exasol> for Months {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        let years = self.0 / 12;
        let months = self.0 % 12;
        buf.append(format_args!("{}-{}", years, months));

        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let num_years = self.0 / 12;
        let precision = num_years
            .unsigned_abs()
            .checked_ilog10()
            .unwrap_or_default()
            + 1;

        Some(ExaTypeInfo::IntervalYearToMonth(IntervalYearToMonth::new(
            precision,
        )))
    }
}

impl<'r> Decode<'r, Exasol> for Months {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        let input = Cow::<str>::deserialize(value.value).map_err(Box::new)?;

        let input_err_fn = || format!("could not parse {input} as INTERVAL YEAR TO MONTH");

        let (years, months) = input.rsplit_once('-').ok_or_else(input_err_fn)?;

        let years = years.parse::<i32>().map_err(Box::new)?;
        let months = months.parse::<i32>().map_err(Box::new)?;

        // The number of months will always get decoded as being positive.
        // So the sign of the years determines how to add up the months.
        let total_months = match years.is_negative() {
            true => years * 12 - months,
            false => years * 12 + months,
        };

        Ok(Months::new(total_months))
    }
}

impl From<Months> for chrono::Months {
    fn from(value: Months) -> Self {
        chrono::Months::new(value.0.unsigned_abs())
    }
}

impl<Tz: TimeZone> Add<Months> for DateTime<Tz> {
    type Output = DateTime<Tz>;

    fn add(self, rhs: Months) -> Self::Output {
        if rhs.0.is_negative() {
            self.checked_sub_months(rhs.into()).unwrap()
        } else {
            self.checked_add_months(rhs.into()).unwrap()
        }
    }
}

impl Add<Months> for NaiveDate {
    type Output = NaiveDate;

    fn add(self, rhs: Months) -> Self::Output {
        if rhs.0.is_negative() {
            self.checked_sub_months(rhs.into()).unwrap()
        } else {
            self.checked_add_months(rhs.into()).unwrap()
        }
    }
}

impl Add<Months> for NaiveDateTime {
    type Output = NaiveDateTime;

    fn add(self, rhs: Months) -> Self::Output {
        if rhs.0.is_negative() {
            self.checked_sub_months(rhs.into()).unwrap()
        } else {
            self.checked_add_months(rhs.into()).unwrap()
        }
    }
}

impl<Tz: TimeZone> Sub<Months> for DateTime<Tz> {
    type Output = DateTime<Tz>;

    fn sub(self, rhs: Months) -> Self::Output {
        if rhs.0.is_negative() {
            self.checked_add_months(rhs.into()).unwrap()
        } else {
            self.checked_sub_months(rhs.into()).unwrap()
        }
    }
}

impl Sub<Months> for NaiveDate {
    type Output = NaiveDate;

    fn sub(self, rhs: Months) -> Self::Output {
        if rhs.0.is_negative() {
            self.checked_add_months(rhs.into()).unwrap()
        } else {
            self.checked_sub_months(rhs.into()).unwrap()
        }
    }
}

impl Sub<Months> for NaiveDateTime {
    type Output = NaiveDateTime;

    fn sub(self, rhs: Months) -> Self::Output {
        if rhs.0.is_negative() {
            self.checked_add_months(rhs.into()).unwrap()
        } else {
            self.checked_sub_months(rhs.into()).unwrap()
        }
    }
}
