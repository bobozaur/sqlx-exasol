use std::borrow::Cow;

use serde::Deserialize;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{arguments::ExaBuffer, database::Exasol, type_info::ExaTypeInfo, value::ExaValueRef};

impl Type<Exasol> for str {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Varchar(Default::default())
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for &'_ str {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        // Exasol treats empty strings as NULL
        if self.is_empty() {
            buf.append(());
            return IsNull::Yes;
        }

        buf.append(self);
        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(<Self as Type<Exasol>>::type_info())
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

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <str as Type<Exasol>>::compatible(ty)
    }
}

impl Encode<'_, Exasol> for String {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        <&str as Encode<Exasol>>::encode(&**self, buf)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        <&str as Encode<Exasol>>::produces(&&**self)
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
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        match self {
            Cow::Borrowed(str) => <&str as Encode<Exasol>>::encode(*str, buf),
            Cow::Owned(str) => <&str as Encode<Exasol>>::encode(&**str, buf),
        }
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        <&str as Encode<Exasol>>::produces(&&**self)
    }
}

impl<'r> Decode<'r, Exasol> for Cow<'r, str> {
    fn decode(value: ExaValueRef<'r>) -> Result<Self, BoxDynError> {
        Cow::deserialize(value.value).map_err(From::from)
    }
}
