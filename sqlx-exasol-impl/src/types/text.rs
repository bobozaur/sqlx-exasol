use std::fmt::Display;

use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::{Text, Type},
};

use crate::{arguments::ExaBuffer, types::ExaHasArrayType, ExaTypeInfo, ExaValueRef, Exasol};

impl<T> Type<Exasol> for Text<T> {
    fn type_info() -> ExaTypeInfo {
        <String as Type<Exasol>>::type_info()
    }
}

impl<T> ExaHasArrayType for Text<T> where T: ExaHasArrayType {}

impl<T> Encode<'_, Exasol> for Text<T>
where
    T: Display,
{
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        let prev_len = buf.buffer.len();
        buf.append(format_args!("{}", self.0))?;

        // Serializing an empty string would result in just the quotes being added to the buffer.
        // Important because Exasol treats empty strings as NULL.
        if buf.buffer.len() - prev_len == 2 {
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

#[cfg(test)]
mod tests {
    use sqlx::{types::Text, Encode};

    use crate::{ExaArguments, Exasol};

    #[test]
    fn test_text_null_string() {
        let mut arg_buffer = ExaArguments::default();
        let value = Text(String::new());
        let is_null = Encode::<Exasol>::encode_by_ref(&value, &mut arg_buffer.buf).unwrap();

        assert!(is_null.is_null());
    }

    #[test]
    fn test_text_null_str() {
        let mut arg_buffer = ExaArguments::default();
        let value = Text("");
        let is_null = Encode::<Exasol>::encode_by_ref(&value, &mut arg_buffer.buf).unwrap();

        assert!(is_null.is_null());
    }

    #[test]
    fn test_text_non_null_string() {
        let mut arg_buffer = ExaArguments::default();
        let value = Text(String::from("something"));
        let is_null = Encode::<Exasol>::encode_by_ref(&value, &mut arg_buffer.buf).unwrap();

        assert!(!is_null.is_null());
    }

    #[test]
    fn test_text_non_null_str() {
        let mut arg_buffer = ExaArguments::default();
        let value = Text("something");
        let is_null = Encode::<Exasol>::encode_by_ref(&value, &mut arg_buffer.buf).unwrap();

        assert!(!is_null.is_null());
    }
}
