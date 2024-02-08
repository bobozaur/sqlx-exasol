CREATE TABLE users
(
    user_id  INTEGER IDENTITY PRIMARY KEY,
    username VARCHAR(16) NOT NULL
);

-- Not meant to do anything, but just test that query separation
-- in migrations is done properly.
DELETE FROM users WHERE 1 = 1;