use serde::Deserialize;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{arguments::ExaBuffer, database::Exasol, type_info::ExaTypeInfo, value::ExaValueRef};

use super::ExaParameter;

impl ExaParameter for bool {}

impl Type<Exasol> for bool {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Boolean
    }
}

impl Encode<'_, Exasol> for bool {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaTypeInfo::Boolean)
    }
}

impl Decode<'_, Exasol> for bool {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}
