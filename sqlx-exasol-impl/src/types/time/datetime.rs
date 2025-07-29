use ::serde::{Deserialize, Serialize};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};
use time::{serde, OffsetDateTime, PrimitiveDateTime};

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{ExaDataType, ExaTypeInfo},
    value::ExaValueRef,
};

impl Type<Exasol> for PrimitiveDateTime {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::TimestampWithLocalTimeZone.into()
    }
}

impl Encode<'_, Exasol> for PrimitiveDateTime {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(PrimitiveDateTimeSer(self))?;
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

impl Decode<'_, Exasol> for PrimitiveDateTime {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        PrimitiveDateTimeDe::deserialize(value.value)
            .map(|v| v.0)
            .map_err(From::from)
    }
}

impl Type<Exasol> for OffsetDateTime {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Timestamp.into()
    }
}

impl Encode<'_, Exasol> for OffsetDateTime {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(OffsetDateTimeSer(self))?;
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

impl<'r> Decode<'r, Exasol> for OffsetDateTime {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        OffsetDateTimeDe::deserialize(value.value)
            .map(|v| v.0)
            .map_err(From::from)
    }
}

#[derive(Serialize)]
struct PrimitiveDateTimeSer<'a>(
    #[serde(serialize_with = "timestamp::serialize")] &'a PrimitiveDateTime,
);

#[derive(Deserialize)]
struct PrimitiveDateTimeDe(#[serde(deserialize_with = "timestamp::deserialize")] PrimitiveDateTime);

#[derive(Serialize)]
struct OffsetDateTimeSer<'a>(
    #[serde(serialize_with = "timestamptz::serialize")] &'a OffsetDateTime,
);

#[derive(Deserialize)]
struct OffsetDateTimeDe(#[serde(deserialize_with = "timestamptz::deserialize")] OffsetDateTime);

serde::format_description!(
    timestamp,
    PrimitiveDateTime,
    "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:9]"
);
serde::format_description!(
    timestamptz,
    OffsetDateTime,
    "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:9]"
);
