CREATE TABLE post
(
    post_id    INTEGER IDENTITY PRIMARY KEY,
    user_id    INTEGER NOT NULL CONSTRAINT POST_USER REFERENCES users (user_id),
    content    VARCHAR(2000000) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Not meant to do anything, but just test that query separation
-- in migrations is done properly.
-- We include the semicolon in the where clause because we test based on that.
-- NOTE: Putting a semicolon in a comment such as this one will cause the migration
--       to fail, because Exasol accepts a statement that is just a comment, but the 
--       second  part of the comment, after the semicolon, will always fail.
DELETE FROM post WHERE ';' = ';'  
;
               
     
