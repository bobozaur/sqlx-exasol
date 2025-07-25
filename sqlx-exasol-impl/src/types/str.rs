use std::borrow::Cow;

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
    type_info::{Charset, ExaDataType, ExaTypeInfo, StringLike},
    value::ExaValueRef,
};

impl Type<Exasol> for str {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Varchar(StringLike {
            size: StringLike::MAX_VARCHAR_LEN,
            character_set: Some(Charset::Utf8),
        })
        .into()
    }
}

impl Encode<'_, Exasol> for &'_ str {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        // Exasol treats empty strings as NULL
        if self.is_empty() {
            buf.append(())?;
            return Ok(IsNull::Yes);
        }

        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // 2 quotes + length
        2 + self.len()
    }
}

impl<'r> Decode<'r, Exasol> for &'r str {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        <&str>::deserialize(value.value).map_err(From::from)
    }
}

impl Type<Exasol> for String {
    fn type_info() -> ExaTypeInfo {
        <str as Type<Exasol>>::type_info()
    }
}

impl Encode<'_, Exasol> for String {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        <&str as Encode<Exasol>>::encode(self.as_ref(), buf)
    }

    fn size_hint(&self) -> usize {
        <&str as Encode<Exasol>>::size_hint(&self.as_ref())
    }
}

impl Decode<'_, Exasol> for String {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <&str as Decode<Exasol>>::decode(value).map(ToOwned::to_owned)
    }
}

impl Encode<'_, Exasol> for Cow<'_, str> {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        <&str as Encode<Exasol>>::encode(self.as_ref(), buf)
    }

    fn size_hint(&self) -> usize {
        <&str as Encode<Exasol>>::size_hint(&self.as_ref())
    }
}
