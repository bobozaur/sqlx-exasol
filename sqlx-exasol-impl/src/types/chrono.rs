use std::{
    borrow::Cow,
    ops::{Add, Sub},
};

use serde::Deserialize;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::{
        chrono::{DateTime, Local, NaiveDate, NaiveDateTime, TimeZone, Utc},
        Type,
    },
};

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{ExaDataType, ExaTypeInfo, IntervalDayToSecond, IntervalYearToMonth},
    value::ExaValueRef,
};

const TIMESTAMP_FMT: &str = "%Y-%m-%d %H:%M:%S%.6f";

impl Type<Exasol> for DateTime<Utc> {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Timestamp.into()
    }
}

impl Encode<'_, Exasol> for DateTime<Utc> {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        Encode::<Exasol>::encode(self.naive_utc(), buf)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaDataType::Timestamp.into())
    }

    fn size_hint(&self) -> usize {
        TIMESTAMP_FMT.len()
    }
}

impl<'r> Decode<'r, Exasol> for DateTime<Utc> {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        let naive: NaiveDateTime = Decode::<Exasol>::decode(value)?;
        Ok(DateTime::from_naive_utc_and_offset(naive, Utc))
    }
}

impl Type<Exasol> for DateTime<Local> {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::TimestampWithLocalTimeZone.into()
    }
}

impl Encode<'_, Exasol> for DateTime<Local> {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        Encode::<Exasol>::encode(self.naive_local(), buf)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaDataType::TimestampWithLocalTimeZone.into())
    }

    fn size_hint(&self) -> usize {
        TIMESTAMP_FMT.len()
    }
}

impl<'r> Decode<'r, Exasol> for DateTime<Local> {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        let naive: NaiveDateTime = Decode::<Exasol>::decode(value)?;
        naive
            .and_local_timezone(Local)
            .single()
            .ok_or("cannot uniquely determine timezone offset")
            .map_err(From::from)
    }
}

impl Type<Exasol> for NaiveDateTime {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Timestamp.into()
    }
}

impl Encode<'_, Exasol> for NaiveDateTime {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(format_args!("{}", self.format(TIMESTAMP_FMT)))?;
        Ok(IsNull::No)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaDataType::Timestamp.into())
    }

    fn size_hint(&self) -> usize {
        TIMESTAMP_FMT.len()
    }
}

impl Decode<'_, Exasol> for NaiveDateTime {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        let input = Cow::<str>::deserialize(value.value).map_err(Box::new)?;
        Self::parse_from_str(&input, TIMESTAMP_FMT)
            .map_err(Box::new)
            .map_err(From::from)
    }
}

impl Type<Exasol> for chrono::Duration {
    fn type_info() -> ExaTypeInfo {
        let ids = IntervalDayToSecond::new(
            IntervalDayToSecond::MAX_PRECISION,
            IntervalDayToSecond::MAX_SUPPORTED_FRACTION,
        );
        ExaDataType::IntervalDayToSecond(ids).into()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for chrono::Duration {
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

    fn produces(&self) -> Option<ExaTypeInfo> {
        let precision = self
            .num_days()
            .unsigned_abs()
            .checked_ilog10()
            .unwrap_or_default()
            + 1;

        let fraction = (self.num_milliseconds() % 1000)
            .unsigned_abs()
            .checked_ilog10()
            .map(|v| v + 1)
            .unwrap_or_default();

        Some(ExaDataType::IntervalDayToSecond(IntervalDayToSecond::new(precision, fraction)).into())
    }

    fn size_hint(&self) -> usize {
        // 1 sign + max days precision + 1 space + 2 hours + 1 column + 2 minutes + 1 column + 2
        // seconds + 1 dot + max milliseconds fraction
        1 + IntervalDayToSecond::MAX_PRECISION as usize
            + 10
            + IntervalDayToSecond::MAX_SUPPORTED_FRACTION as usize
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

        let days: i64 = days.parse().map_err(Box::new)?;
        let hours: i64 = hours.parse().map_err(Box::new)?;
        let minutes: i64 = minutes.parse().map_err(Box::new)?;
        let seconds: i64 = seconds.parse().map_err(Box::new)?;
        let millis: i64 = millis.parse().map_err(Box::new)?;
        let sign = if days.is_negative() { -1 } else { 1 };

        let duration = chrono::Duration::try_days(days).ok_or_else(input_err_fn)?
            + chrono::Duration::try_hours(hours * sign).ok_or_else(input_err_fn)?
            + chrono::Duration::try_minutes(minutes * sign).ok_or_else(input_err_fn)?
            + chrono::Duration::try_seconds(seconds * sign).ok_or_else(input_err_fn)?
            + chrono::Duration::try_milliseconds(millis * sign).ok_or_else(input_err_fn)?;

        Ok(duration)
    }
}

impl Type<Exasol> for NaiveDate {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Date.into()
    }
}

impl Encode<'_, Exasol> for NaiveDate {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaDataType::Date.into())
    }

    fn size_hint(&self) -> usize {
        // 4 year + 1 dash + 2 months + 1 dash + 2 days
        10
    }
}

impl Decode<'_, Exasol> for NaiveDate {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}

/// A duration in calendar months, analog to [`chrono::Months`].
/// Unlike [`chrono::Months`], this type can represent a negative duration.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
pub struct Months(i32);

impl Months {
    #[must_use]
    pub const fn new(num: i32) -> Self {
        Self(num)
    }

    #[must_use]
    pub fn num_months(&self) -> i32 {
        self.0
    }
}

impl Type<Exasol> for Months {
    fn type_info() -> ExaTypeInfo {
        let iym = IntervalYearToMonth::new(IntervalYearToMonth::MAX_PRECISION);
        ExaDataType::IntervalYearToMonth(iym).into()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for Months {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        let years = self.0 / 12;
        let months = (self.0 % 12).abs();
        buf.append(format_args!("{years}-{months}"))?;

        Ok(IsNull::No)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let num_years = self.0 / 12;
        let precision = num_years
            .unsigned_abs()
            .checked_ilog10()
            .unwrap_or_default()
            + 1;

        Some(ExaDataType::IntervalYearToMonth(IntervalYearToMonth::new(precision)).into())
    }

    fn size_hint(&self) -> usize {
        // 1 sign + max year precision + 1 dash + 2 months
        1 + IntervalYearToMonth::MAX_PRECISION as usize + 1 + 2
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
        let total_months = if years.is_negative() {
            years * 12 - months
        } else {
            years * 12 + months
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
