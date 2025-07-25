use std::ops::Range;

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
    type_info::{Decimal, ExaDataType, ExaTypeInfo},
    value::ExaValueRef,
};

/// Numbers within this range must be serialized/deserialized as integers.
/// The ones above/under these thresholds are treated as strings.
const NUMERIC_I64_RANGE: Range<i64> = -999_999_999_999_999_999..1_000_000_000_000_000_000;

impl Type<Exasol> for i8 {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Decimal(Decimal {
            precision: Some(Decimal::MAX_8BIT_PRECISION),
            scale: 0,
        })
        .into()
    }
}

impl Encode<'_, Exasol> for i8 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // sign + max num digits
        1 + Decimal::MAX_8BIT_PRECISION as usize
    }
}

impl Decode<'_, Exasol> for i8 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}

impl Type<Exasol> for i16 {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Decimal(Decimal {
            precision: Some(Decimal::MAX_16BIT_PRECISION),
            scale: 0,
        })
        .into()
    }
}

impl Encode<'_, Exasol> for i16 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // sign + max num digits
        1 + Decimal::MAX_16BIT_PRECISION as usize
    }
}

impl Decode<'_, Exasol> for i16 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}

impl Type<Exasol> for i32 {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Decimal(Decimal {
            precision: Some(Decimal::MAX_32BIT_PRECISION),
            scale: 0,
        })
        .into()
    }
}

impl Encode<'_, Exasol> for i32 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // sign + max num digits
        1 + Decimal::MAX_32BIT_PRECISION as usize
    }
}

impl Decode<'_, Exasol> for i32 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}

impl Type<Exasol> for i64 {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Decimal(Decimal {
            precision: Some(Decimal::MAX_64BIT_PRECISION),
            scale: 0,
        })
        .into()
    }
}

impl Encode<'_, Exasol> for i64 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        if NUMERIC_I64_RANGE.contains(self) {
            buf.append(self)?;
        } else {
            // Large numbers get serialized as strings
            buf.append(format_args!("{self}"))?;
        }

        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // 1 quote + 1 sign + max num digits + 1 quote
        2 + Decimal::MAX_64BIT_PRECISION as usize + 1
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
