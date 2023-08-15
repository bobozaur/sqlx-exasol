use serde::Deserialize;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};
use uuid::Uuid;

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{ExaDataType, ExaTypeInfo},
    value::ExaValueRef,
};

impl Type<Exasol> for Uuid {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Hashtype(Default::default()).into()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for Uuid {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaDataType::Hashtype(Default::default()).into())
    }
}

impl Decode<'_, Exasol> for Uuid {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}
