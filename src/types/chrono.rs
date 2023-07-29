use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{database::Exasol, type_info::ExaTypeInfo, value::ExaValueRef};

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

impl_encode_decode!(NaiveDate);
impl_encode_decode!(NaiveDateTime);
