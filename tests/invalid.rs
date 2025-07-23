#![cfg(feature = "migrate")]

mod macros;

// Test incompatible types
test_type_invalid!(i16_into_i8<i16>::"DECIMAL(3,0)"::(i16::MAX));
test_type_invalid!(i32_into_i8<i32>::"DECIMAL(3,0)"::(i32::MAX));
test_type_invalid!(i32_into_i16<i32>::"DECIMAL(5,0)"::(i32::MAX));
test_type_invalid!(i64_into_i8<i64>::"DECIMAL(3,0)"::(i64::MAX));
test_type_invalid!(i64_into_i16<i64>::"DECIMAL(5,0)"::(i64::MAX));
test_type_invalid!(i64_into_i32<i64>::"DECIMAL(10,0)"::(i64::MAX));

// Not enough room due to scale eating up space
test_type_invalid!(i16_no_room<i16>::"DECIMAL(5,2)"::(i16::MAX));
test_type_invalid!(i32_no_room<i32>::"DECIMAL(10,2)"::(i32::MAX));
test_type_invalid!(i64_no_room<i64>::"DECIMAL(20,2)"::(i64::MAX));

// Not enough room due to number being too large
test_type_invalid!(f64_no_scale<f64>::"DECIMAL(20,0)"::(f64::MAX));

test_type_invalid!(i64_into_string<i64>::"VARCHAR(100)"::(i64::MAX));
test_type_invalid!(string_into_i64<String>::"DECIMAL(20,0)"::("some_value"));
test_type_invalid!(array_string_into_i64<String>::"DECIMAL(20,0)"::(vec!["some_value", "some_other_value"]));
test_type_invalid!(array_i32_into_string<i32>::"VARCHAR(100)"::(vec![1, 2, 3]));
test_type_invalid!(null_in_non_nullable<Option<i32>>::"DECIMAL(10,0) NOT NULL"::(vec![None, Some(1), Some(2), Some(3)]));
test_type_invalid!(null_without_option<i32>::"DECIMAL(10,0)"::(vec![None, Some(1), Some(2), Some(3)]));
