use sqlx_exasol::types::Uuid;

use crate::{test_type_array, test_type_valid};

test_type_valid!(uuid<Uuid>::"HASHTYPE(16 BYTE)"::(format!("'{}'", Uuid::from_u64_pair(12_345_789, 12_345_789)) => Uuid::from_u64_pair(12_345_789, 12_345_789)));
test_type_valid!(uuid_option<Option<Uuid>>::"HASHTYPE(16 BYTE)"::("NULL" => None::<Uuid>, "''" => None::<Uuid>, format!("'{}'", Uuid::from_u64_pair(12_345_789, 12_345_789)) => Some(Uuid::from_u64_pair(12_345_789, 12_345_789))));
test_type_array!(uuid_array<Uuid>::"HASHTYPE(16 BYTE)"::(vec![Uuid::from_u64_pair(12_345_789, 12_345_789), Uuid::from_u64_pair(12_345_789, 12_345_789), Uuid::from_u64_pair(12_345_789, 12_345_789)]));
