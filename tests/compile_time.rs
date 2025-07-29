#![cfg(feature = "migrate")]
#![cfg(feature = "macros")]

extern crate sqlx_exasol as sqlx;

mod macros;

use sqlx::migrate::Migrator;
use sqlx_exasol::Type;

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
