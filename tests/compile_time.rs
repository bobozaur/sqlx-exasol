extern crate sqlx_exasol as sqlx;

mod macros;

test_compile_time_type!(
    bool,
    true,
    "INSERT INTO compile_time_tests (column_bool) VALUES(?);",
    "SELECT column_bool FROM compile_time_tests;"
);

test_compile_time_type!(
    i8,
    i8::MAX,
    "INSERT INTO compile_time_tests (column_i8) VALUES(?);",
    "SELECT column_i8 FROM compile_time_tests;"
);

test_compile_time_type!(
    i16,
    i16::MAX,
    "INSERT INTO compile_time_tests (column_i16) VALUES(?);",
    "SELECT column_i16 FROM compile_time_tests;"
);

test_compile_time_type!(
    i32,
    i32::MAX,
    "INSERT INTO compile_time_tests (column_i32) VALUES(?);",
    "SELECT column_i32 FROM compile_time_tests;"
);

test_compile_time_type!(
    i64,
    i64::MAX,
    "INSERT INTO compile_time_tests (column_i64) VALUES(?);",
    "SELECT column_i64 FROM compile_time_tests;"
);

test_compile_time_type!(
    f64,
    f64::MAX,
    "INSERT INTO compile_time_tests (column_f64) VALUES(?);",
    "SELECT column_f64 FROM compile_time_tests;"
);

test_compile_time_type!(
    char,
    String,
    String::new(),
    "INSERT INTO compile_time_tests (column_char) VALUES(?);",
    "SELECT column_char FROM compile_time_tests;"
);

test_compile_time_type!(
    varchar,
    String,
    String::new(),
    "INSERT INTO compile_time_tests (column_varchar) VALUES(?);",
    "SELECT column_varchar FROM compile_time_tests;"
);
