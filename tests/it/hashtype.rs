use sqlx_exasol::types::HashType;

use crate::{test_type_array, test_type_valid};

test_type_valid!(hashtype<HashType>::"HASHTYPE (15 BYTE)"::("'550e8400e29b11d4a7164466554400'" => HashType(String::from("550e8400e29b11d4a7164466554400"))));

test_type_array!(hashtype_array<HashType>::"HASHTYPE (15 BYTE)"::(vec![HashType(String::from("550e8400e29b11d4a7164466554400")), HashType(String::from("550e8400e29b11d4a7164466554400"))]));
