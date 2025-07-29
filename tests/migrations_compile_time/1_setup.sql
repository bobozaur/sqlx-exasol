CREATE TABLE compile_time_tests
(
    column_bool BOOLEAN,
    column_i8 DECIMAL(3, 0),
    column_i16 DECIMAL(5, 0),
    column_i32 DECIMAL(10, 0),
    column_i64 DECIMAL(20, 0),
    column_decimal DECIMAL(36, 28),
    column_f64 DOUBLE PRECISION,
    column_char_utf8 CHAR(16) UTF8,
    column_varchar_utf8 VARCHAR(16) UTF8,
    column_char_ascii CHAR(16) ASCII,
    column_varchar_ascii VARCHAR(16) ASCII,
    column_date DATE,
    column_timestamp TIMESTAMP,
    column_timestamp_with_timezone TIMESTAMP WITH LOCAL TIME ZONE,
    column_interval_dts INTERVAL DAY TO SECOND,
    column_interval_ytm INTERVAL YEAR TO MONTH,
    column_geometry GEOMETRY,
    column_uuid HASHTYPE(16 BYTE)
);
