use serde::Deserialize;
use serde_json::{json, Value};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};
use uuid::Uuid;

use crate::{
    database::Exasol,
    type_info::{ExaTypeInfo, Hashtype},
    value::ExaValueRef,
};

impl Type<Exasol> for Uuid {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Hashtype(Hashtype::new(32))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        match ty {
            ExaTypeInfo::Char(_) | ExaTypeInfo::Varchar(_) => true,
            ExaTypeInfo::Hashtype(h) if h.size() == 32 => true,
            _ => false,
        }
    }
}

impl Encode<'_, Exasol> for Uuid {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        buf.push([json!(self)]);
        IsNull::No
    }
}

impl Decode<'_, Exasol> for Uuid {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        Uuid::deserialize(value.value).map_err(From::from)
    }
}
