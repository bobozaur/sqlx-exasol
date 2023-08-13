-- https://github.com/prisma/database-schema-examples/tree/master/postgres/basic-twitter#basic-twitter
CREATE TABLE tweet
(
    id         INTEGER IDENTITY PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    text       VARCHAR(200000) NOT NULL,
    owner_id   INTEGER
);

CREATE TABLE tweet_reply
(
    id         INTEGER IDENTITY,
    tweet_id   INTEGER NOT NULL CONSTRAINT tweet_id_fk REFERENCES tweet (id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    text       VARCHAR(200000) NOT NULL,
    owner_id   INTEGER
);

CREATE TABLE products (
    product_no INTEGER,
    name VARCHAR(2000000),
    price NUMERIC
);