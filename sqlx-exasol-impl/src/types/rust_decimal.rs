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
    value::ExaValueRef,
};

impl Type<Exasol> for rust_decimal::Decimal {
    #[allow(clippy::cast_possible_truncation)]
    fn type_info() -> ExaTypeInfo {
        // A somewhat non-sensical value used to allow decoding any DECIMAL value
        // with a supported scale.
        ExaDataType::Decimal(Decimal {
            precision: None,
            scale: rust_decimal::Decimal::MAX_SCALE as u8,
        })
        .into()
    }
}

impl Encode<'_, Exasol> for rust_decimal::Decimal {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> Result<IsNull, BoxDynError> {
        buf.append(format_args!("{self}"))?;
        Ok(IsNull::No)
    }

    fn size_hint(&self) -> usize {
        // 1 quote + 1 sign + 1 zero + 1 dot + max scale + 1 quote
        4 + Decimal::MAX_SCALE as usize + 1
    }
}

impl Decode<'_, Exasol> for rust_decimal::Decimal {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}
