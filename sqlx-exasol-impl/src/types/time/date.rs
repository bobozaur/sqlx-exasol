use ::serde::{Deserialize, Serialize};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};
use time::{serde, Date};

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{ExaDataType, ExaTypeInfo},
    types::ExaHasArrayType,
    value::ExaValueRef,
};

impl Type<Exasol> for Date {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Date.into()
    }
}

impl ExaHasArrayType for Date {}

impl Encode<'_, Exasol> for Date {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(DateSer(self))?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // 2 quotes + 4 year + 1 dash + 2 months + 1 dash + 2 days
        12
    }
}

impl Decode<'_, Exasol> for Date {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        DateDe::deserialize(value.value)
            .map(|v| v.0)
            .map_err(From::from)
    }
}

#[derive(Serialize)]
struct DateSer<'a>(#[serde(serialize_with = "date::serialize")] &'a Date);

#[derive(Deserialize)]
struct DateDe(#[serde(deserialize_with = "date::deserialize")] Date);

serde::format_description!(date, Date, "[year]-[month]-[day]");
