use serde_json::{json, Value};
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;

use crate::database::Exasol;
use crate::type_info::{Decimal, ExaTypeInfo};
use crate::value::ExaValueRef;
use serde::Deserialize;

impl Type<Exasol> for u8 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(3, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Decimal(d) if d.precision() <= 3 && d.scale() == 0)
    }
}

impl Type<Exasol> for u16 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(5, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Decimal(d) if d.precision() <= 5 && d.scale() == 0)
    }
}

impl Type<Exasol> for u32 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(10, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Decimal(d) if d.precision() <= 10 && d.scale() == 0)
    }
}

impl Type<Exasol> for u64 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(20, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Decimal(d) if d.precision() <= 20 && d.scale() == 0)
    }
}

impl Encode<'_, Exasol> for u8 {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        buf.push([json!(self)]);
        IsNull::No
    }
}

impl Encode<'_, Exasol> for u16 {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        buf.push([json!(self)]);
        IsNull::No
    }
}

impl Encode<'_, Exasol> for u32 {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        buf.push([json!(self)]);
        IsNull::No
    }
}

impl Encode<'_, Exasol> for u64 {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        buf.push([json!(self)]);
        IsNull::No
    }
}

impl Decode<'_, Exasol> for u8 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        u8::deserialize(value.value).map_err(From::from)
    }
}

impl Decode<'_, Exasol> for u16 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        u16::deserialize(value.value).map_err(From::from)
    }
}

impl Decode<'_, Exasol> for u32 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        u32::deserialize(value.value).map_err(From::from)
    }
}

impl Decode<'_, Exasol> for u64 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        u64::deserialize(value.value).map_err(From::from)
    }
}
