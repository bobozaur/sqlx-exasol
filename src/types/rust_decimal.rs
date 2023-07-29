use sqlx_core::types::Type;

use crate::{
    database::Exasol,
    type_info::{Decimal, ExaTypeInfo},
};

use super::impl_encode_decode;

impl Type<Exasol> for rust_decimal::Decimal {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Decimal(Decimal::new(0, 0))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        matches!(ty, ExaTypeInfo::Decimal(_))
    }
}

impl_encode_decode!(rust_decimal::Decimal);
