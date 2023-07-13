use serde::Deserialize;
use sqlx::{encode::IsNull, error::BoxDynError, Decode, Encode, Type};
use std::borrow::Cow;

use crate::{
    database::Exasol,
    type_info::{DataType, ExaTypeInfo},
    value::ExaValueRef,
};

impl Type<Exasol> for str {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::new(DataType::Varchar)
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty.data_type, DataType::Varchar | DataType::Char)
    }
}

impl Encode<'_, Exasol> for &'_ str {
    fn encode_by_ref(&self, args: &mut Vec<String>) -> IsNull {
        let arg = serde_json::to_string(self).expect(concat!(
            "serializing primite ",
            stringify!(String),
            " should work"
        ));
        args.push(arg);

        IsNull::No
    }
}

impl<'r> Decode<'r, Exasol> for &'r str {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        <&str>::deserialize(&value.0.value).map_err(From::from)
    }
}

impl Type<Exasol> for String {
    fn type_info() -> ExaTypeInfo {
        <str as Type<Exasol>>::type_info()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <str as Type<Exasol>>::compatible(ty)
    }
}

impl Encode<'_, Exasol> for String {
    fn encode_by_ref(&self, buf: &mut Vec<String>) -> IsNull {
        <&str as Encode<Exasol>>::encode(&**self, buf)
    }
}

impl Decode<'_, Exasol> for String {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <&str as Decode<Exasol>>::decode(value).map(ToOwned::to_owned)
    }
}

impl Type<Exasol> for Cow<'_, str> {
    fn type_info() -> ExaTypeInfo {
        <&str as Type<Exasol>>::type_info()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <&str as Type<Exasol>>::compatible(ty)
    }
}

impl Encode<'_, Exasol> for Cow<'_, str> {
    fn encode_by_ref(&self, buf: &mut Vec<String>) -> IsNull {
        match self {
            Cow::Borrowed(str) => <&str as Encode<Exasol>>::encode(*str, buf),
            Cow::Owned(str) => <&str as Encode<Exasol>>::encode(&**str, buf),
        }
    }
}

impl<'r> Decode<'r, Exasol> for Cow<'r, str> {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        Cow::deserialize(&value.0.value).map_err(From::from)
    }
}
