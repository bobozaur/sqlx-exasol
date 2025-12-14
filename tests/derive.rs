#![cfg(all(feature = "migrate", feature = "macros"))]

use sqlx_exasol::types::{ExaHasArrayType, ExaIter, Type};

#[derive(Type)]
#[sqlx(transparent)]
struct DeriveType(i8);

impl ExaHasArrayType for DeriveType {}

/// Would not compile if [`DeriveType`] does not implement [`sqlx_exasol::types::ExaHasArrayType`]
#[test]
fn test_exa_has_array_type() {
    ExaIter::new(&[DeriveType(0), DeriveType(1)]);
}
