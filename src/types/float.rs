use serde::Deserialize;
use serde_json::{json, Value};
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::Type;

use crate::database::Exasol;
use crate::type_info::ExaTypeInfo;
use crate::value::ExaValueRef;

impl Type<Exasol> for f32 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Double
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        match ty {
            ExaTypeInfo::Double => true,
            ExaTypeInfo::Decimal(d) if d.scale() > 0 => true,
            _ => false,
        }
    }
}

impl Type<Exasol> for f64 {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Double
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        match ty {
            ExaTypeInfo::Double => true,
            ExaTypeInfo::Decimal(d) if d.scale() > 0 => true,
            _ => false,
        }
    }
}

impl Encode<'_, Exasol> for f32 {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        // NaN is treated as NULL by Exasol.
        // Infinity is not supported by Exasol but serde_json
        // serializes it as NULL as well.
        if self.is_finite() {
            buf.push([json!(self)]);
            IsNull::No
        } else {
            buf.push([Value::Null]);
            IsNull::Yes
        }
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaTypeInfo::Double)
    }
}

impl Decode<'_, Exasol> for f32 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}

impl Encode<'_, Exasol> for f64 {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        // NaN is treated as NULL by Exasol.
        // Infinity is not supported by Exasol but serde_json
        // serializes it as NULL as well.
        if self.is_finite() {
            buf.push([json!(self)]);
            IsNull::No
        } else {
            buf.push([Value::Null]);
            IsNull::Yes
        }
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaTypeInfo::Double)
    }
}

impl Decode<'_, Exasol> for f64 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}
