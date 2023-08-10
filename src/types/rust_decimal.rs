use std::ops::Range;

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

/// Numbers within this range must be serialized/deserialized as integers.
/// The ones above/under these thresholds are treated as strings.
const NUMERIC_DECIMAL_RANGE: Range<rust_decimal::Decimal> =
    rust_decimal::Decimal::from_parts(2808348671, 232830643, 0, true, 0)
        ..rust_decimal::Decimal::from_parts(2808348672, 232830643, 0, false, 0);

impl Type<Exasol> for rust_decimal::Decimal {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(Decimal::MAX_PRECISION, Decimal::MAX_SCALE))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Decimal(_))
    }
}

impl Encode<'_, Exasol> for rust_decimal::Decimal {
    fn encode_by_ref(&self, buf: &mut ExaBuffer) -> IsNull {
        if NUMERIC_DECIMAL_RANGE.contains(self) {
            // We know for sure that the conversion will succeed
            // since we checked the number boundaries
            let val = i64::try_from(*self).unwrap();
            buf.append(val);
        } else {
            // Large numbers get serialized as strings
            buf.append(format_args!("{self}"));
        };

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

#[test]
fn test_decimal_range() {
    let start = rust_decimal::Decimal::new(super::MIN_I64_NUMERIC, 0);
    let end = rust_decimal::Decimal::new(super::MAX_I64_NUMERIC, 0);

    assert_eq!(NUMERIC_DECIMAL_RANGE.start, start);
    assert_eq!(NUMERIC_DECIMAL_RANGE.end, end);
}
