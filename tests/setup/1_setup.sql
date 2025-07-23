-- https://github.com/prisma/database-schema-examples/tree/master/postgres/basic-twitter#basic-twitter
CREATE TABLE tweet
(
    id         DECIMAL(20, 0) IDENTITY PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    text       VARCHAR(2000000) NOT NULL,
    owner_id   DECIMAL(20, 0)
);

CREATE TABLE tweet_reply
(
    id         DECIMAL(20, 0) IDENTITY,
    tweet_id   DECIMAL(20, 0) NOT NULL CONSTRAINT tweet_id_fk REFERENCES tweet (id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    text       VARCHAR(2000000) NOT NULL,
    owner_id   DECIMAL(20, 0)
);

CREATE TABLE products (
    product_no DECIMAL(20, 0),
    name VARCHAR(2000000),
    price DECIMAL(20, 0)
);