INSERT INTO post (post_id, user_id, content, created_at)
VALUES (1,
        1,
        'This new computer is lightning-fast!',
        CURRENT_TIMESTAMP - TO_DSINTERVAL('0 1:00:00.000')),
       (2,
        2,
        '@alice is a haxxor :(',
        CURRENT_TIMESTAMP - TO_DSINTERVAL('0 0:30:00.000'));