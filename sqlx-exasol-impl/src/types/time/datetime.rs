use ::serde::{Deserialize, Serialize};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};
use time::{serde, OffsetDateTime, PrimitiveDateTime, UtcOffset};

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{ExaDataType, ExaTypeInfo},
    types::ExaHasArrayType,
    value::ExaValueRef,
};

impl Type<Exasol> for PrimitiveDateTime {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::TimestampWithLocalTimeZone.into()
    }
}

impl Type<Exasol> for OffsetDateTime {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Timestamp.into()
    }
}

impl ExaHasArrayType for PrimitiveDateTime {}
impl ExaHasArrayType for OffsetDateTime {}

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

impl Encode<'_, Exasol> for OffsetDateTime {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        let utc_dt = self.to_offset(UtcOffset::UTC);
        let primitive = PrimitiveDateTime::new(utc_dt.date(), utc_dt.time());
        primitive.encode(buf)
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

impl<'r> Decode<'r, Exasol> for OffsetDateTime {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        PrimitiveDateTime::decode(value).map(PrimitiveDateTime::assume_utc)
    }
}

#[derive(Serialize)]
struct PrimitiveDateTimeSer<'a>(
    #[serde(serialize_with = "timestamp::serialize")] &'a PrimitiveDateTime,
);

#[derive(Deserialize)]
struct PrimitiveDateTimeDe(#[serde(deserialize_with = "timestamp::deserialize")] PrimitiveDateTime);

serde::format_description!(
    timestamp,
    PrimitiveDateTime,
    "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]"
);
