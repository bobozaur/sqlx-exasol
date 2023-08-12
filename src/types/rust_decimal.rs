
use serde::Deserialize;
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{
    arguments::ExaBuffer,
    database::Exasol,
    type_info::{Decimal, ExaTypeInfo},
    value::ExaValueRef,
};

/// Scale limit set by `rust_decimal` crate.
const RUST_DECIMAL_MAX_SCALE: u32 = 28;

impl Type<Exasol> for rust_decimal::Decimal {
    fn type_info() -> ExaTypeInfo {
        // This is not a valid Exasol datatype defintion,
        // but defining it like this means that we can accommodate
        // almost any DECIMAL value when decoding
        // (considering `rust_decimal` scale limitations)
        ExaTypeInfo::Decimal(Decimal::new(
            Decimal::MAX_PRECISION + RUST_DECIMAL_MAX_SCALE,
            RUST_DECIMAL_MAX_SCALE,
        ))
    }
    fn compatible(ty: &ExaTypeInfo) -> bool {
        <Self as Type<Exasol>>::type_info().compatible(ty)
    }
}

impl Encode<'_, Exasol> for rust_decimal::Decimal {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        buf.append(format_args!("{self}"));
        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let scale = self.scale();
        let precision = self
            .mantissa()
            .unsigned_abs()
            .checked_ilog10()
            .unwrap_or_default()
            + 1;
        Some(ExaTypeInfo::Decimal(Decimal::new(precision, scale)))
    }
}

impl Decode<'_, Exasol> for rust_decimal::Decimal {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}
