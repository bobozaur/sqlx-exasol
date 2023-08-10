use serde::Deserialize;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};
use uuid::Uuid;

use crate::{arguments::ExaBuffer, database::Exasol, type_info::ExaTypeInfo, value::ExaValueRef};

impl Type<Exasol> for Uuid {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Hashtype(Default::default())
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(
            ty,
            ExaTypeInfo::Char(_) | ExaTypeInfo::Varchar(_) | ExaTypeInfo::Hashtype(_)
        )
    }
}

impl Encode<'_, Exasol> for Uuid {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(self);
        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaTypeInfo::Hashtype(Default::default()))
    }
}

impl Decode<'_, Exasol> for Uuid {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}
