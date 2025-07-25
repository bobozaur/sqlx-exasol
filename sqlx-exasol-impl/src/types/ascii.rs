use std::borrow::Cow;

pub use ascii::*;
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

impl Type<Exasol> for AsciiStr {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Varchar(StringLike {
            size: StringLike::MAX_VARCHAR_LEN,
            character_set: Some(Charset::Ascii),
        })
        .into()
    }
}

impl Encode<'_, Exasol> for &'_ AsciiStr {
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

impl<'r> Decode<'r, Exasol> for &'r AsciiStr {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        <&AsciiStr>::deserialize(value.value).map_err(From::from)
    }
}

impl Type<Exasol> for AsciiString {
    fn type_info() -> ExaTypeInfo {
        <AsciiStr as Type<Exasol>>::type_info()
    }
}

impl Encode<'_, Exasol> for AsciiString {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        <&AsciiStr as Encode<Exasol>>::encode(self.as_ref(), buf)
    }

    fn size_hint(&self) -> usize {
        <&AsciiStr as Encode<Exasol>>::size_hint(&self.as_ref())
    }
}

impl Decode<'_, Exasol> for AsciiString {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <&AsciiStr as Decode<Exasol>>::decode(value).map(ToOwned::to_owned)
    }
}

impl Encode<'_, Exasol> for Cow<'_, AsciiStr> {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        <&AsciiStr as Encode<Exasol>>::encode(self.as_ref(), buf)
    }

    fn size_hint(&self) -> usize {
        <&AsciiStr as Encode<Exasol>>::size_hint(&self.as_ref())
    }
}
