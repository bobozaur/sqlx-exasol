use std::fmt::Display;

use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::{Text, Type},
};

use crate::{arguments::ExaBuffer, ExaTypeInfo, ExaValueRef, Exasol};

impl<T> Type<Exasol> for Text<T> {
    fn type_info() -> ExaTypeInfo {
        <String as Type<Exasol>>::type_info()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <String as Type<Exasol>>::compatible(ty)
    }
}

impl<'q, T> Encode<'q, Exasol> for Text<T>
where
    T: Display,
{
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        let prev_len = buf.inner.len();
        buf.append(format_args!("{}", self.0))?;

        // Serializing an empty string would result in just the quotes being added to the buffer.
        // Important because Exasol treats empty strings as NULL.
        if buf.inner.len() - prev_len == 2 {
            Ok(IsNull::Yes)
        } else {
            // Otherwise, the resulted text was not an empty string.
            Ok(IsNull::No)
        }
    }
}

impl<'r, T> Decode<'r, Exasol> for Text<T>
where
    for<'a> T: TryFrom<&'a str>,
    for<'a> BoxDynError: From<<T as TryFrom<&'a str>>::Error>,
{
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        let s: &str = Decode::<Exasol>::decode(value)?;
        Ok(Self(s.try_into()?))
    }
}
