use serde::{Deserialize, Serialize};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::{Json, Type},
};

use crate::{
    arguments::ExaBuffer,
    type_info::{ExaDataType, StringLike},
    ExaTypeInfo, ExaValueRef, Exasol,
};

impl<T> Type<Exasol> for Json<T> {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Varchar(StringLike {
            size: StringLike::MAX_VARCHAR_LEN,
            character_set: None,
        })
        .into()
    }
}

impl<T> Encode<'_, Exasol> for Json<T>
where
    T: Serialize,
{
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        let value = serde_json::to_string(&self.0)?;
        buf.append(&value)?;
        Ok(IsNull::No)
    }
}

impl<'r, T> Decode<'r, Exasol> for Json<T>
where
    T: 'r + Deserialize<'r>,
{
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        <&str>::deserialize(value.value)
            .and_then(serde_json::from_str)
            .map(Json)
            .map_err(From::from)
    }
}
