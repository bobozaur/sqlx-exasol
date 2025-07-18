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
    type_info::{ExaDataType, ExaTypeInfo, HashType},
    value::ExaValueRef,
};

impl Type<Exasol> for Uuid {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::HashType(HashType {}).into()
    }
}

impl Encode<'_, Exasol> for Uuid {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self.simple())?;
        Ok(IsNull::No)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaDataType::HashType(HashType {}).into())
    }

    fn size_hint(&self) -> usize {
        // 16 bytes encoded as HEX, so double
        32
    }
}

impl Decode<'_, Exasol> for Uuid {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}
