CREATE TABLE post
(
    post_id    INTEGER IDENTITY PRIMARY KEY,
    user_id    INTEGER NOT NULL CONSTRAINT POST_USER REFERENCES users (user_id),
    content    VARCHAR(2000000) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Not meant to do anything, but just test that query separation
-- in migrations is done properly.
DELETE FROM post WHERE ';' = ';';
