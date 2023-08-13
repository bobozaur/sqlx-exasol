CREATE TABLE migrations_simple_test (
    some_id INTEGER PRIMARY KEY,
    some_payload INTEGER NOT NUll
);

INSERT INTO migrations_simple_test (some_id, some_payload)
VALUES (1, 100);