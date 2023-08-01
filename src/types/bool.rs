use serde::Deserialize;
use serde_json::{json, Value};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{database::Exasol, type_info::ExaTypeInfo, value::ExaValueRef};

impl Type<Exasol> for bool {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Boolean
    }
}

impl Encode<'_, Exasol> for bool {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        buf.push([json!(self)]);
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
