use serde::Deserialize;
use serde_json::{json, Value};
use sqlx_core::{
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    types::Type,
};

use crate::{
    database::Exasol,
    type_info::{Decimal, ExaTypeInfo},
    value::ExaValueRef,
};

impl Type<Exasol> for rust_decimal::Decimal {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(36, 35))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Decimal(_))
    }
}

impl Encode<'_, Exasol> for rust_decimal::Decimal {
    fn encode_by_ref(&self, buf: &mut Vec<[Value; 1]>) -> IsNull {
        let value = if self < &rust_decimal::Decimal::new(1000000000000000000, 0)
            && &rust_decimal::Decimal::new(-1000000000000000000, 0) < self
        {
            // We know for sure that the conversion will succeed
            // since we checked the number boundaries
            json!(i64::try_from(*self).unwrap())
        } else {
            // Large numbers get serialized as strings
            Value::String(self.to_string())
        };

        buf.push([value]);
        IsNull::No
    }

    fn produces(&self) -> Option<ExaTypeInfo> {
        let scale = self.scale();
        let precision = self
            .mantissa()
            .unsigned_abs()
            .checked_ilog10()
            .unwrap_or_default()
            + 1
            - scale;
        Some(ExaTypeInfo::Decimal(Decimal::new(precision, scale)))
    }
}

impl Decode<'_, Exasol> for rust_decimal::Decimal {
    fn decode(value: ExaValueRef<'_>) -> Result<Self, BoxDynError> {
        <Self as Deserialize>::deserialize(value.value).map_err(From::from)
    }
}
