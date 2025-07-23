use serde::{Deserialize, Serialize};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::{Json, Type},
};

use crate::{
    arguments::ExaBuffer,
    type_info::{Charset, ExaDataType, StringLike},
    ExaTypeInfo, ExaValueRef, Exasol,
};

impl<T> Type<Exasol> for Json<T> {
    fn type_info() -> ExaTypeInfo {
        let string_like = StringLike::new(StringLike::MAX_VARCHAR_LEN, Charset::Utf8);
        ExaDataType::Varchar(string_like).into()
    }
}

impl<T> Encode<'_, Exasol> for Json<T>
where
    T: Serialize,
{
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(&self.0)?;
        Ok(IsNull::No)
    }
}

impl<'r, T> Decode<'r, Exasol> for Json<T>
where
    T: 'r + Deserialize<'r>,
{
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        T::deserialize(value.value).map(Json).map_err(From::from)
    }
}
