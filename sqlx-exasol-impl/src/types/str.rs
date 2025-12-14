use std::{borrow::Cow, rc::Rc, sync::Arc};

use serde::Deserialize;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    forward_encode_impl,
    types::Type,
};

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{Charset, ExaDataType, ExaTypeInfo},
    types::ExaHasArrayType,
    value::ExaValueRef,
};

impl Type<Exasol> for str {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Varchar {
            size: ExaDataType::VARCHAR_MAX_LEN,
            character_set: Charset::Utf8,
        }
        .into()
    }
}

impl Type<Exasol> for String {
    fn type_info() -> ExaTypeInfo {
        <str as Type<Exasol>>::type_info()
    }
}

impl ExaHasArrayType for &str {}
impl ExaHasArrayType for String {}
impl ExaHasArrayType for Box<str> {}
impl ExaHasArrayType for Rc<str> {}
impl ExaHasArrayType for Arc<str> {}

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

impl Decode<'_, Exasol> for String {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <&str as Decode<Exasol>>::decode(value).map(ToOwned::to_owned)
    }
}

forward_encode_impl!(String, &str, Exasol);
forward_encode_impl!(Cow<'_, str>, &str, Exasol);
forward_encode_impl!(Box<str>, &str, Exasol);
forward_encode_impl!(Rc<str>, &str, Exasol);
forward_encode_impl!(Arc<str>, &str, Exasol);
