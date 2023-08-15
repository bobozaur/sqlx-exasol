use super::*;
use exasol::ExaIter;
use std::collections::HashSet;

test_type_valid!(bool::"BOOLEAN"::(false, true));
test_type_valid!(bool_option<Option<bool>>::"BOOLEAN"::("NULL" => None::<bool>, "true" => Some(true)));
test_type_array!(bool_array<bool>::"BOOLEAN"::(vec![true, false], Vec::<bool>::new(), Some(vec![true, false]), [false; 4], &[false; 4], vec![true, false].into_boxed_slice(), ExaIter::from(HashSet::from([true, false, true]).iter())));
test_type_array!(bool_array_option<Option<bool>>::"BOOLEAN"::(vec![Some(true), Some(false), None]));
test_type_array!(bool_empty_array<Option<bool>>::"BOOLEAN"::(Vec::<bool>::new()));
