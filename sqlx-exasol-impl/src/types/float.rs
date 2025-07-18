use serde::Deserialize;
use serde_json::Value;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{ExaDataType, ExaTypeInfo},
    value::ExaValueRef,
};

impl Type<Exasol> for f32 {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Double.into()
    }
}

impl Encode<'_, Exasol> for f32 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        // NaN is treated as NULL by Exasol.
        // Infinity is not supported by Exasol but serde_json
        // serializes it as NULL as well.
        if self.is_finite() {
            buf.append(self)?;
            Ok(IsNull::No)
        } else {
            buf.append(())?;
            Ok(IsNull::Yes)
        }
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaDataType::Double.into())
    }
}

impl Decode<'_, Exasol> for f32 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        match value.value {
            Value::Number(n) => <Self as Deserialize>::deserialize(n).map_err(From::from),
            Value::String(s) => serde_json::from_str(s).map_err(From::from),
            v => Err(format!("invalid f32 value: {v}").into()),
        }
    }
}

impl Type<Exasol> for f64 {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Double.into()
    }
}

impl Encode<'_, Exasol> for f64 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        // NaN is treated as NULL by Exasol.
        // Infinity is not supported by Exasol but serde_json
        // serializes it as NULL as well.
        if self.is_finite() {
            buf.append(self)?;
            Ok(IsNull::No)
        } else {
            buf.append(())?;
            Ok(IsNull::Yes)
        }
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        Some(ExaDataType::Double.into())
    }
}

impl Decode<'_, Exasol> for f64 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        match value.value {
            Value::Number(n) => <Self as Deserialize>::deserialize(n).map_err(From::from),
            Value::String(s) => serde_json::from_str(s).map_err(From::from),
            v => Err(format!("invalid f64 value: {v}").into()),
        }
    }
}
