CREATE TABLE compile_time_tests
(
    column_bool BOOLEAN,
    column_i8 DECIMAL(3, 0),
    column_i16 DECIMAL(5, 0),
    column_i32 DECIMAL(10, 0),
    column_i64 DECIMAL(20, 0),
    column_f64 DOUBLE PRECISION,
    column_char CHAR(16),
    column_varchar VARCHAR(16),
    column_date DATE,
    column_timestamp TIMESTAMP,
    column_timestamp_with_timezone TIMESTAMP WITH LOCAL TIME ZONE,
    column_duration INTERVAL DAY TO SECOND,
    column_months INTERVAL YEAR TO MONTH,
    column_geometry GEOMETRY,
    column_uuid HASHTYPE(16 BYTE),
);
