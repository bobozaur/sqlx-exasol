use super::*;

const MAX_U64_NUMERIC: u64 = 1000000000000000000;

test_type_valid!(u8::"DECIMAL(3, 0)"::(u8::MIN, u8::MAX));
test_type_valid!(u8_into_smaller<u8>::"DECIMAL(1, 0)"::(u8::MIN, 5));
test_type_valid!(u16::"DECIMAL(5, 0)"::(u16::MIN, u16::MAX, u16::from(u8::MAX)));
test_type_valid!(u16_into_smaller<u16>::"DECIMAL(3, 0)"::(u16::MIN, 20));
test_type_valid!(u8_in_u16<u16>::"DECIMAL(5, 0)"::(u8::MIN => u16::from(u8::MIN), u8::MAX => u16::from(u8::MAX)));
test_type_valid!(u32::"DECIMAL(10, 0)"::(u32::MIN, u32::MAX, u32::from(u8::MAX), u32::from(u16::MAX)));
test_type_valid!(u32_into_smaller<u32>::"DECIMAL(7, 0)"::(u32::MIN, 12345));
test_type_valid!(u8_in_u32<u32>::"DECIMAL(10, 0)"::(u8::MIN => u32::from(u8::MIN), u8::MAX => u32::from(u8::MAX)));
test_type_valid!(u16_in_u32<u32>::"DECIMAL(10, 0)"::(u16::MIN => u32::from(u16::MIN), u16::MAX => u32::from(u16::MAX)));
test_type_valid!(u64::"DECIMAL(20, 0)"::(u64::MIN, u64::MAX, u64::from(u8::MAX), u64::from(u16::MAX), u64::from(u32::MAX), MAX_U64_NUMERIC, MAX_U64_NUMERIC - 1));
test_type_valid!(u64_into_smaller<u64>::"DECIMAL(15, 0)"::(u64::MIN, 1234567890));
test_type_valid!(u8_in_u64<u64>::"DECIMAL(20, 0)"::(u8::MIN => u64::from(u8::MIN), u8::MAX => u64::from(u8::MAX)));
test_type_valid!(u16_in_u64<u64>::"DECIMAL(20, 0)"::(u16::MIN => u64::from(u16::MIN), u16::MAX => u64::from(u16::MAX)));
test_type_valid!(u32_in_u64<u64>::"DECIMAL(20, 0)"::(u32::MIN => u64::from(u32::MIN), u32::MAX => u64::from(u32::MAX)));
test_type_valid!(u64_option<Option<u64>>::"DECIMAL(20, 0)"::("NULL" => None::<u64>, u64::MAX => Some(u64::MAX)));
test_type_array!(u64_array<u64>::"DECIMAL(20, 0)"::(vec![u64::MIN, u64::MAX, 1234567]));
