use crate::{test_type_array, test_type_valid};

const MIN_I64_NUMERIC: i64 = -999_999_999_999_999_999;
const MAX_I64_NUMERIC: i64 = 1_000_000_000_000_000_000;

test_type_valid!(i8::"DECIMAL(3, 0)"::(i8::MIN, i8::MAX));

test_type_valid!(i16::"DECIMAL(5, 0)"::(i16::MIN, i16::MAX, i16::from(i8::MIN), i16::from(i8::MAX)));
test_type_valid!(i8_in_i16<i16>::"DECIMAL(5, 0)"::(i8::MIN => i16::from(i8::MIN), i8::MAX => i16::from(i8::MAX)));

test_type_valid!(i32::"DECIMAL(10, 0)"::(i32::MIN, i32::MAX, i32::from(i8::MIN), i32::from(i8::MAX), i32::from(i16::MIN), i32::from(i16::MAX)));
test_type_valid!(i8_in_i32<i32>::"DECIMAL(10, 0)"::(i8::MIN => i32::from(i8::MIN), i8::MAX => i32::from(i8::MAX)));
test_type_valid!(i16_in_i32<i32>::"DECIMAL(10, 0)"::(i16::MIN => i32::from(i16::MIN), i16::MAX => i32::from(i16::MAX)));

test_type_valid!(i64::"DECIMAL(20, 0)"::(i64::MIN, i64::MAX, i64::from(i8::MIN), i64::from(i8::MAX), i64::from(i16::MIN), i64::from(i16::MAX), i64::from(i32::MIN), i64::from(i32::MAX), MIN_I64_NUMERIC, MIN_I64_NUMERIC - 1, MAX_I64_NUMERIC, MAX_I64_NUMERIC - 1));
test_type_valid!(i8_in_i64<i64>::"DECIMAL(20, 0)"::(i8::MIN => i64::from(i8::MIN), i8::MAX => i64::from(i8::MAX)));
test_type_valid!(i16_in_i64<i64>::"DECIMAL(20, 0)"::(i16::MIN => i64::from(i16::MIN), i16::MAX => i64::from(i16::MAX)));
test_type_valid!(i32_in_i64<i64>::"DECIMAL(20, 0)"::(i32::MIN => i64::from(i32::MIN), i32::MAX => i64::from(i32::MAX)));

test_type_valid!(i64_option<Option<i64>>::"DECIMAL(20, 0)"::("NULL" => None::<i64>, i64::MAX => Some(i64::MAX)));
test_type_array!(i64_array<i64>::"DECIMAL(20, 0)"::(vec![i64::MIN, i64::MAX, 1_234_567]));
