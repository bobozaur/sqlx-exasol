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

const MIN_I64_NUMERIC: i64 = -999_999_999_999_999_999;
const MAX_I64_NUMERIC: i64 = 1_000_000_000_000_000_000;
const MIN_I128_NUMERIC: i128 = -999_999_999_999_999_999;
const MAX_I128_NUMERIC: i128 = 1_000_000_000_000_000_000;

/// Numbers within this range must be serialized/deserialized as integers.
/// The ones above/under these thresholds are treated as strings.
const NUMERIC_I64_RANGE: Range<i64> = MIN_I64_NUMERIC..MAX_I64_NUMERIC;
const NUMERIC_I128_RANGE: Range<i128> = MIN_I128_NUMERIC..MAX_I128_NUMERIC;

impl Type<Exasol> for i8 {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Decimal(Decimal::new(Decimal::MAX_8BIT_PRECISION, 0)).into()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for i8 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let precision = self.unsigned_abs().checked_ilog10().unwrap_or_default() + 1;
        Some(ExaDataType::Decimal(Decimal::new(precision, 0)).into())
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
        ExaDataType::Decimal(Decimal::new(Decimal::MAX_16BIT_PRECISION, 0)).into()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for i16 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let precision = self.unsigned_abs().checked_ilog10().unwrap_or_default() + 1;
        Some(ExaDataType::Decimal(Decimal::new(precision, 0)).into())
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
        ExaDataType::Decimal(Decimal::new(Decimal::MAX_32BIT_PRECISION, 0)).into()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for i32 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(self)?;
        Ok(IsNull::No)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let precision = self.unsigned_abs().checked_ilog10().unwrap_or_default() + 1;
        Some(ExaDataType::Decimal(Decimal::new(precision, 0)).into())
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
        ExaDataType::Decimal(Decimal::new(Decimal::MAX_64BIT_PRECISION, 0)).into()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
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

    fn produces(&self) -> Option<ExaTypeInfo> {
        let precision = self.unsigned_abs().checked_ilog10().unwrap_or_default() + 1;
        Some(ExaDataType::Decimal(Decimal::new(precision, 0)).into())
    }

    fn size_hint(&self) -> usize {
        // sign + max num digits
        1 + Decimal::MAX_64BIT_PRECISION as usize
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

impl Type<Exasol> for i128 {
    fn type_info() -> ExaTypeInfo {
        ExaDataType::Decimal(Decimal::new(Decimal::MAX_128BIT_PRECISION, 0)).into()
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for i128 {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        if NUMERIC_I128_RANGE.contains(self) {
            buf.append(self)?;
        } else {
            // Large numbers get serialized as strings
            buf.append(format_args!("{self}"))?;
        }

        Ok(IsNull::No)
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let precision = self.unsigned_abs().checked_ilog10().unwrap_or_default() + 1;
        Some(ExaDataType::Decimal(Decimal::new(precision, 0)).into())
    }

    fn size_hint(&self) -> usize {
        // sign + max num digits
        1 + Decimal::MAX_128BIT_PRECISION as usize
    }
}

impl Decode<'_, Exasol> for i128 {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        match value.value {
            Value::Number(n) => <Self as Deserialize>::deserialize(n).map_err(From::from),
            Value::String(s) => serde_json::from_str(s).map_err(From::from),
            v => Err(format!("invalid i128 value: {v}").into()),
        }
    }
}
