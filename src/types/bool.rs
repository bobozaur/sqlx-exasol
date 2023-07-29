use sqlx_core::types::Type;

use crate::{database::Exasol, type_info::ExaTypeInfo};

use super::impl_encode_decode;

impl Type<Exasol> for bool {
    fn type_info() -> ExaTypeInfo {
        ExaTypeInfo::Boolean
    }
}

impl_encode_decode!(bool);
