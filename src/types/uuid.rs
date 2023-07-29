use sqlx_core::types::Type;
use uuid::Uuid;

use crate::{
    database::Exasol,
    type_info::{ExaTypeInfo, Hashtype},
};

use super::impl_encode_decode;

impl Type<Exasol> for Uuid {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Hashtype(Hashtype::new(32))
    }

    fn compatible(ty: &ExaTypeInfo) -> bool {
        match ty {
            ExaTypeInfo::Char(_) | ExaTypeInfo::Varchar(_) => true,
            ExaTypeInfo::Hashtype(h) if h.size() == 32 => true,
            _ => false,
        }
    }
}

impl_encode_decode!(Uuid);
