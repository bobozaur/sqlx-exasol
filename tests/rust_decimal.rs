#![cfg(feature = "migrate")]
#![cfg(feature = "rust_decimal")]

mod macros;

use sqlx_exasol::types::Decimal;

test_type_valid!(rust_decimal_i64<Decimal>::"DECIMAL(36, 16)"::(Decimal::new(i64::MIN, 16), Decimal::new(i64::MAX, 16), Decimal::new(i64::MAX, 10), Decimal::new(i64::MAX, 5), Decimal::new(i64::MAX, 0)));
test_type_valid!(rust_decimal_i16<Decimal>::"DECIMAL(36, 16)"::(Decimal::new(i64::from(i16::MIN), 5), Decimal::new(i64::from(i16::MAX), 5), Decimal::new(i64::from(i16::MIN), 0), Decimal::new(i64::from(i16::MAX), 0)));
test_type_valid!(rust_decimal_no_scale<Decimal>::"DECIMAL(36, 0)"::(Decimal::new(-340_282_346_638_529, 0), Decimal::new(340_282_346_638_529, 0), Decimal::new(0, 0)));
test_type_valid!(rust_decimal_option<Option<Decimal>>::"DECIMAL(36, 16)"::("NULL" => None::<Decimal>, Decimal::new(i64::from(i16::MIN), 5) => Some(Decimal::new(i64::from(i16::MIN), 5))));
test_type_array!(rust_decimal_array<Decimal>::"DECIMAL(36, 16)"::(vec![Decimal::new(i64::MIN, 16), Decimal::new(i64::MAX, 16), Decimal::new(i64::MAX, 10), Decimal::new(i64::MAX, 5), Decimal::new(i64::MAX, 0)]));

test_type_valid!(i8_decimal_scale<Decimal>::"DECIMAL(5, 2)"::(i8::MIN => Decimal::from(i8::MIN), i8::MAX => Decimal::from(i8::MAX)));
test_type_valid!(i16_decimal_scale<Decimal>::"DECIMAL(7, 2)"::(i16::MIN => Decimal::from(i16::MIN), i16::MAX => Decimal::from(i16::MAX)));
test_type_valid!(i32_decimal_scale<Decimal>::"DECIMAL(12, 2)"::(i32::MIN => Decimal::from(i32::MIN), i32::MAX => Decimal::from(i32::MAX)));
test_type_valid!(i64_decimal_scale<Decimal>::"DECIMAL(22, 2)"::(i64::MIN => Decimal::from(i64::MIN), i64::MAX => Decimal::from(i64::MAX)));
