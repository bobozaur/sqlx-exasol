use chrono::{DateTime, Local, NaiveDateTime, Utc};
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
    value::ExaValueRef,
};

const TIMESTAMP_FMT: &str = "%Y-%m-%d %H:%M:%S%.9f";

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

    fn size_hint(&self) -> usize {
        // 1 quote +
        // 4 years + 1 dash + 2 months + 1 dash + 2 days +
        // 1 space + 2 hours + 2 minutes + 2 seconds + 9 subseconds +
        // 1 quote
        28
    }
}

impl Decode<'_, Exasol> for NaiveDateTime {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        let input = <&str>::deserialize(value.value).map_err(Box::new)?;
        Self::parse_from_str(input, TIMESTAMP_FMT)
            .map_err(Box::new)
            .map_err(From::from)
    }
}

impl Type<Exasol> for DateTime<Utc> {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Timestamp.into()
    }
}

impl Encode<'_, Exasol> for DateTime<Utc> {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        Encode::<Exasol>::encode(self.naive_utc(), buf)
    }

    fn size_hint(&self) -> usize {
        // 1 quote +
        // 4 years + 1 dash + 2 months + 1 dash + 2 days +
        // 1 space + 2 hours + 2 minutes + 2 seconds + 9 subseconds +
        // 1 quote
        28
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

    fn size_hint(&self) -> usize {
        // 1 quote +
        // 4 years + 1 dash + 2 months + 1 dash + 2 days +
        // 1 space + 2 hours + 2 minutes + 2 seconds + 9 subseconds +
        // 1 quote
        28
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
