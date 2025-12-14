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
    types::ExaHasArrayType,
    value::ExaValueRef,
};

impl Type<Exasol> for bool {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Boolean.into()
    }
}

impl ExaHasArrayType for bool {}

impl Encode<'_, Exasol> for bool {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // len of `false`
        5
    }
}

impl Decode<'_, Exasol> for bool {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}
