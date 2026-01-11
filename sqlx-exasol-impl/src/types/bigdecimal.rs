use bigdecimal::BigDecimal;
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
    type_info::{Decimal, ExaDataType, ExaTypeInfo},
    types::ExaHasArrayType,
    value::ExaValueRef,
};

impl Type<Exasol> for BigDecimal {
    fn type_info() -> ExaTypeInfo {
        // A somewhat non-sensical value used to allow decoding any DECIMAL value.
        ExaDataType::Decimal(Decimal {
            precision: None,
            scale: Decimal::MAX_SCALE,
        })
        .into()
    }
}

impl ExaHasArrayType for BigDecimal {}

impl Encode<'_, Exasol> for BigDecimal {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(format_args!("{self}"))?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // 1 quote + 1 sign + 1 zero + 1 dot + max scale + 1 quote
        4 + Decimal::MAX_SCALE as usize + 1
    }
}

impl Decode<'_, Exasol> for BigDecimal {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}
