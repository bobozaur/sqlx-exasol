use sqlx_core::types::Type;

use crate::database::Exasol;
use crate::type_info::ExaTypeInfo;

use super::impl_encode_decode;

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

impl_encode_decode!(f32);
impl_encode_decode!(f64);
