use super::*;

// Test incompatible types
test_type_invalid!(u16_into_u8<u16>::"DECIMAL(3,0)"::(u16::MAX));
test_type_invalid!(u32_into_u8<u32>::"DECIMAL(3,0)"::(u32::MAX));
test_type_invalid!(u32_into_u16<u32>::"DECIMAL(5,0)"::(u32::MAX));
test_type_invalid!(u64_into_u8<u64>::"DECIMAL(3,0)"::(u64::MAX));
test_type_invalid!(u64_into_u16<u64>::"DECIMAL(5,0)"::(u64::MAX));
test_type_invalid!(u64_into_u32<u64>::"DECIMAL(10,0)"::(u64::MAX));
test_type_invalid!(u128_into_u8<u128>::"DECIMAL(3,0)"::(340_282_366_920_938_463_463_374_607_431_768_211u128));
test_type_invalid!(u128_into_u16<u128>::"DECIMAL(5,0)"::(340_282_366_920_938_463_463_374_607_431_768_211u128));
test_type_invalid!(u128_into_u32<u128>::"DECIMAL(10,0)"::(340_282_366_920_938_463_463_374_607_431_768_211u128));
test_type_invalid!(u128_into_u64<u128>::"DECIMAL(20,0)"::(340_282_366_920_938_463_463_374_607_431_768_211u128));

test_type_invalid!(i16_into_i8<i16>::"DECIMAL(3,0)"::(i16::MAX));
test_type_invalid!(i32_into_i8<i32>::"DECIMAL(3,0)"::(i32::MAX));
test_type_invalid!(i32_into_i16<i32>::"DECIMAL(5,0)"::(i32::MAX));
test_type_invalid!(i64_into_i8<i64>::"DECIMAL(3,0)"::(i64::MAX));
test_type_invalid!(i64_into_i16<i64>::"DECIMAL(5,0)"::(i64::MAX));
test_type_invalid!(i64_into_i32<i64>::"DECIMAL(10,0)"::(i64::MAX));
test_type_invalid!(i128_into_i8<i128>::"DECIMAL(3,0)"::(340_282_366_920_938_463_463_374_607_431_768_211i128));
test_type_invalid!(i128_into_i16<i128>::"DECIMAL(5,0)"::(340_282_366_920_938_463_463_374_607_431_768_211i128));
test_type_invalid!(i128_into_i32<i128>::"DECIMAL(10,0)"::(340_282_366_920_938_463_463_374_607_431_768_211i128));
test_type_invalid!(i128_into_i64<i128>::"DECIMAL(20,0)"::(340_282_366_920_938_463_463_374_607_431_768_211i128));

// Not enough room due to scale eating up space
test_type_invalid!(u16_no_room<u16>::"DECIMAL(5,2)"::(u16::MAX));
test_type_invalid!(u32_no_room<u32>::"DECIMAL(10,2)"::(u32::MAX));
test_type_invalid!(u64_no_room<u64>::"DECIMAL(20,2)"::(u64::MAX));
test_type_invalid!(i16_no_room<i16>::"DECIMAL(5,2)"::(i16::MAX));
test_type_invalid!(i32_no_room<i32>::"DECIMAL(10,2)"::(i32::MAX));
test_type_invalid!(i64_no_room<i64>::"DECIMAL(20,2)"::(i64::MAX));

// Not enough room due to number being too large
test_type_invalid!(u128_no_room<u128>::"DECIMAL(36,0)"::(u128::MAX));
test_type_invalid!(i128_no_room<i128>::"DECIMAL(36,0)"::(i128::MAX));

test_type_invalid!(f32_no_scale<f32>::"DECIMAL(20,0)"::(f32::MAX));
test_type_invalid!(f64_no_scale<f64>::"DECIMAL(20,0)"::(f64::MAX));

test_type_invalid!(u64_into_string<u64>::"VARCHAR(100)"::(u64::MAX));
test_type_invalid!(i64_into_string<i64>::"VARCHAR(100)"::(i64::MAX));
test_type_invalid!(string_into_u64<String>::"DECIMAL(20,0)"::("some_value"));
test_type_invalid!(array_string_into_u64<String>::"DECIMAL(20,0)"::(vec!["some_value", "some_other_value"]));
test_type_invalid!(array_i32_into_string<u64>::"VARCHAR(100)"::(vec![1, 2, 3]));
test_type_invalid!(null_in_non_nullable<Option<i32>>::"DECIMAL(10,0) NOT NULL"::(vec![None, Some(1), Some(2), Some(3)]));
test_type_invalid!(null_without_option<i32>::"DECIMAL(10,0)"::(vec![None, Some(1), Some(2), Some(3)]));
