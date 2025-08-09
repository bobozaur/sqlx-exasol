#![allow(deprecated)]

use sqlx::migrate::Migrator;
use sqlx_exasol::{types::ExaIntervalYearToMonth, Type};

use crate::test_compile_time_type;

#[allow(dead_code)]
static MIGRATOR: Migrator = sqlx_exasol::migrate!("tests/migrations_compile_time");

#[derive(Type)]
#[sqlx(transparent)]
struct Stuff(i8);

test_compile_time_type!(
    bool,
    bool,
    true,
    "INSERT INTO compile_time_tests (column_bool) VALUES(?);",
    "SELECT column_bool FROM compile_time_tests;"
);

test_compile_time_type!(
    i8,
    i8,
    i8::MAX,
    "INSERT INTO compile_time_tests (column_i8) VALUES(?);",
    "SELECT column_i8 FROM compile_time_tests;"
);

test_compile_time_type!(
    i16,
    i16,
    i16::MAX,
    "INSERT INTO compile_time_tests (column_i16) VALUES(?);",
    "SELECT column_i16 FROM compile_time_tests;"
);

test_compile_time_type!(
    i32,
    i32,
    i32::MAX,
    "INSERT INTO compile_time_tests (column_i32) VALUES(?);",
    "SELECT column_i32 FROM compile_time_tests;"
);

test_compile_time_type!(
    i64,
    i64,
    i64::MAX,
    "INSERT INTO compile_time_tests (column_i64) VALUES(?);",
    "SELECT column_i64 FROM compile_time_tests;"
);

test_compile_time_type!(
    f64,
    f64,
    10342342.34324,
    "INSERT INTO compile_time_tests (column_f64) VALUES(?);",
    "SELECT column_f64 FROM compile_time_tests;"
);

test_compile_time_type!(
    char_utf8,
    String,
    String::new(),
    "INSERT INTO compile_time_tests (column_char_utf8) VALUES(?);",
    "SELECT column_char_utf8 FROM compile_time_tests;"
);

test_compile_time_type!(
    varchar_utf8,
    String,
    String::new(),
    "INSERT INTO compile_time_tests (column_varchar_utf8) VALUES(?);",
    "SELECT column_varchar_utf8 FROM compile_time_tests;"
);

test_compile_time_type!(
    char_ascii,
    String,
    String::new(),
    "INSERT INTO compile_time_tests (column_char_ascii) VALUES(?);",
    "SELECT column_char_ascii FROM compile_time_tests;"
);

test_compile_time_type!(
    varchar_ascii,
    String,
    String::new(),
    "INSERT INTO compile_time_tests (column_varchar_ascii) VALUES(?);",
    "SELECT column_varchar_ascii FROM compile_time_tests;"
);

test_compile_time_type!(
    interval_ytm,
    ExaIntervalYearToMonth,
    ExaIntervalYearToMonth(-12),
    "INSERT INTO compile_time_tests (column_interval_ytm) VALUES(?);",
    "SELECT column_interval_ytm FROM compile_time_tests;"
);

test_compile_time_type!(
    hashtype,
    sqlx_exasol::types::HashType,
    sqlx_exasol::types::HashType(String::from("550e8400e29b11d4a7164466554400")),
    "INSERT INTO compile_time_tests (column_hashtype) VALUES(?);",
    "SELECT column_hashtype FROM compile_time_tests;"
);

#[cfg(feature = "time")]
test_compile_time_type!(
    time_date,
    sqlx_exasol::types::time::Date,
    sqlx_exasol::types::time::Date::from_calendar_date(2000, 10.try_into().unwrap(), 10).unwrap(),
    "INSERT INTO compile_time_tests (column_date) VALUES(?);",
    "SELECT column_date FROM compile_time_tests;"
);

#[cfg(all(feature = "chrono", not(feature = "time")))]
test_compile_time_type!(
    chrono_date,
    sqlx_exasol::types::chrono::NaiveDate,
    sqlx_exasol::types::chrono::NaiveDate::from_ymd_opt(2000, 10, 10).unwrap(),
    "INSERT INTO compile_time_tests (column_date) VALUES(?);",
    "SELECT column_date FROM compile_time_tests;"
);

#[cfg(feature = "bigdecimal")]
test_compile_time_type!(
    bigdecimal,
    sqlx_exasol::types::BigDecimal,
    sqlx_exasol::types::BigDecimal::new(2000u16.into(), 10),
    "INSERT INTO compile_time_tests (column_decimal) VALUES(?);",
    "SELECT column_decimal FROM compile_time_tests;"
);

#[cfg(all(feature = "rust_decimal", not(feature = "bigdecimal")))]
test_compile_time_type!(
    rust_decimal,
    sqlx_exasol::types::Decimal,
    sqlx_exasol::types::Decimal::new(2000, 10),
    "INSERT INTO compile_time_tests (column_decimal) VALUES(?);",
    "SELECT column_decimal FROM compile_time_tests;"
);

#[cfg(feature = "uuid")]
test_compile_time_type!(
    uuid,
    sqlx_exasol::types::Uuid,
    sqlx_exasol::types::Uuid::from_u64_pair(12_345_789, 12_345_789),
    "INSERT INTO compile_time_tests (column_uuid) VALUES(?);",
    "SELECT column_uuid FROM compile_time_tests;"
);
