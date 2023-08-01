use serde::Deserialize;
use serde_json::{json, Value};
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;

use crate::database::Exasol;
use crate::type_info::{Decimal, ExaTypeInfo};
use crate::value::ExaValueRef;

use super::impl_encode_decode;

impl Type<Exasol> for i8 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(3, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Decimal(_))
    }
}

impl Type<Exasol> for i16 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(5, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Decimal(_))
    }
}

impl Type<Exasol> for i32 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(10, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Decimal(_))
    }
}

impl Type<Exasol> for i64 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(20, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Decimal(_))
    }
}

impl_encode_decode!(i8);
impl_encode_decode!(i16);
impl_encode_decode!(i32);

impl Encode<'_, Exasol> for i64 {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        // Large numbers get serialized as strings
        let value = if self < &1000000000000000000 && &-1000000000000000000 < self {
            json!(self)
        } else {
            Value::String(self.to_string())
        };

        buf.push([value]);
        IsNull::No
    }
}

impl Decode<'_, Exasol> for i64 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        match value.value {
            Value::Number(n) => <Self as Deserialize>::deserialize(n).map_err(From::from),
            Value::String(s) => serde_json::from_str(s).map_err(From::from),
            v => Err(format!("invalid i64 value: {v}").into()),
        }
    }
}
