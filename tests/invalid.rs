#![cfg(feature = "migrate")]

mod macros;

// Test columns that cannot support the datatype's max value.
test_type_invalid!(i8_into_smaller<i8>::"DECIMAL(1, 0)"::(-15i8, 15i8));
test_type_invalid!(i16_into_smaller<i16>::"DECIMAL(3, 0)"::(-1_234i16, 1_234i16));
test_type_invalid!(i32_into_smaller<i32>::"DECIMAL(7, 0)"::(-12_345_678i32, 12_345_678i32));
test_type_invalid!(i64_into_smaller<i64>::"DECIMAL(15, 0)"::(-1_234_567_890_123_456i64, 1_234_567_890_123_456i64));

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

// Floats cannot go into DECIMAL as floating-point numbers have different semantics.
test_type_invalid!(f64_decimal<f64>::"DECIMAL(36, 16)"::(-1_005_213.045_654_3, 1_005_213.045_654_3, -1005.0456, 1005.0456, -7462.0, 7462.0));

// Strings into numeric and vice-versa
test_type_invalid!(i64_into_string<i64>::"VARCHAR(100)"::(i64::MAX));
test_type_invalid!(string_into_i64<String>::"DECIMAL(20,0)"::("some_value"));
test_type_invalid!(array_string_into_i64<String>::"DECIMAL(20,0)"::(vec!["some_value", "some_other_value"]));
test_type_invalid!(array_i32_into_string<i32>::"VARCHAR(100)"::(vec![1, 2, 3]));

// Fail when inserting NULL in a NOT NULL column
test_type_invalid!(null_in_non_nullable<Option<i32>>::"DECIMAL(10,0) NOT NULL"::(vec![None, Some(1), Some(2), Some(3)]));

// Fail when decoding NULL
test_type_invalid!(null_without_option<i32>::"DECIMAL(10,0)"::(vec![None, Some(1), Some(2), Some(3)]));

// String into DATE/TIMESTAMP
test_type_invalid!(naive_datetime_str<String>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'"));
test_type_invalid!(naive_date_str<String>::"DATE"::("'2023-08-12'"));
test_type_invalid!(datetime_utc_str<String>::"TIMESTAMP"::("'2023-08-12 19:22:36.591000'"));
test_type_invalid!(datetime_local_str<String>::"TIMESTAMP WITH LOCAL TIME ZONE"::("'2023-08-12 19:22:36.591000'"));

// Declaring a column as ASCII means we can't encode UTF8 strings (runtime error)
test_type_invalid!(utf8_in_ascii<String>::"VARCHAR(100) ASCII"::("first value 🦀", "second value 🦀"));

// Test using strings with interval types.
test_type_invalid!(interval_ytm_str<String>::"INTERVAL YEAR TO MONTH"::("+01-05"));
test_type_invalid!(interval_dts_str<String>::"INTERVAL DAY TO SECOND"::("+10 20:45:50.123"));

// `rust_decimal` has a limited scale and cannot all database values.
#[cfg(feature = "rust_decimal")]
test_type_invalid!(rust_decimal_max_scale<sqlx_exasol::types::Decimal>::"DECIMAL(36, 36)"::(sqlx_exasol::types::Decimal::new(i64::MIN, sqlx_exasol::types::Decimal::MAX_SCALE)));
