CREATE TABLE compile_time_tests
(
    column_bool BOOLEAN,
    column_integer INTEGER,
    column_float DOUBLE PRECISION,
    column_date DATE,
    column_timestamp TIMESTAMP,
    column_timestamp_with_timezone TIMESTAMP WITH LOCAL TIME ZONE,
    column_duration INTERVAL DAY TO SECOND,
    column_months INTERVAL YEAR TO MONTH,
    column_geometry GEOMETRY,
    column_uuid HASHTYPE(16 BYTE),
    column_char CHAR(16),
    column_varchar VARCHAR(16)
);
