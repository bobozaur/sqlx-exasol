use super::*;
use ::uuid::Uuid;

test_type_valid!(uuid<Uuid>::"HASHTYPE(16 BYTE)"::(format!("'{}'", Uuid::from_u64_pair(12345789, 12345789)) => Uuid::from_u64_pair(12345789, 12345789)));
test_type_valid!(uuid_str<String>::"HASHTYPE(16 BYTE)"::("'a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8'" => "a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8", "'a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8'" => "a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8"));
test_type_valid!(uuid_option<Option<Uuid>>::"HASHTYPE(16 BYTE)"::("NULL" => None::<Uuid>, "''" => None::<Uuid>, format!("'{}'", Uuid::from_u64_pair(12345789, 12345789)) => Some(Uuid::from_u64_pair(12345789, 12345789))));
test_type_array!(uuid_array<Uuid>::"HASHTYPE(16 BYTE)"::(vec![Uuid::from_u64_pair(12345789, 12345789), Uuid::from_u64_pair(12345789, 12345789), Uuid::from_u64_pair(12345789, 12345789)]));
