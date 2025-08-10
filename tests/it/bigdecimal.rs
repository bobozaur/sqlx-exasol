use sqlx_exasol::types::BigDecimal;

use crate::{test_type_array, test_type_valid};

test_type_valid!(big_decimal_i64<BigDecimal>::"DECIMAL(36, 16)"::(BigDecimal::new(i64::MIN.into(), 16), BigDecimal::new(i64::MAX.into(), 16), BigDecimal::new(i64::MAX.into(), 10), BigDecimal::new(i64::MAX.into(), 5), BigDecimal::new(i64::MAX.into(), 0)));
test_type_valid!(big_decimal_i16<BigDecimal>::"DECIMAL(36, 16)"::(BigDecimal::new(i16::MIN.into(), 5), BigDecimal::new(i16::MAX.into(), 5), BigDecimal::new(i16::MIN.into(), 0), BigDecimal::new(i16::MAX.into(), 0)));
test_type_valid!(big_decimal_no_scale<BigDecimal>::"DECIMAL(36, 0)"::(BigDecimal::new((-340_282_346_638_529i64).into(), 0), BigDecimal::new(340_282_346_638_529i64.into(), 0), BigDecimal::new(0.into(), 0)));
test_type_valid!(big_decimal_option<Option<BigDecimal>>::"DECIMAL(36, 16)"::("NULL" => None::<BigDecimal>, BigDecimal::new(i16::MIN.into(), 5) => Some(BigDecimal::new(i16::MIN.into(), 5))));
test_type_array!(big_decimal_array<BigDecimal>::"DECIMAL(36, 16)"::(vec![BigDecimal::new(i64::MIN.into(), 16), BigDecimal::new(i64::MAX.into(), 16), BigDecimal::new(i64::MAX.into(), 10), BigDecimal::new(i64::MAX.into(), 5), BigDecimal::new(i64::MAX.into(), 0)]));

test_type_valid!(i8_decimal_scale<BigDecimal>::"DECIMAL(5, 2)"::(i8::MIN => BigDecimal::from(i8::MIN), i8::MAX => BigDecimal::from(i8::MAX)));
test_type_valid!(i16_decimal_scale<BigDecimal>::"DECIMAL(7, 2)"::(i16::MIN => BigDecimal::from(i16::MIN), i16::MAX => BigDecimal::from(i16::MAX)));
test_type_valid!(i32_decimal_scale<BigDecimal>::"DECIMAL(12, 2)"::(i32::MIN => BigDecimal::from(i32::MIN), i32::MAX => BigDecimal::from(i32::MAX)));
test_type_valid!(i64_decimal_scale<BigDecimal>::"DECIMAL(22, 2)"::(i64::MIN => BigDecimal::from(i64::MIN), i64::MAX => BigDecimal::from(i64::MAX)));
