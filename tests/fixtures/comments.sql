INSERT INTO comment(comment_id, post_id, user_id, content, created_at)
VALUES (1,
        1,
        2,
        'lol bet ur still bad, 1v1 me',
        CURRENT_TIMESTAMP - TO_DSINTERVAL('0 0:50:00.000')),
       (2,
        1,
        1,
        'you''re on!',
        CURRENT_TIMESTAMP - TO_DSINTERVAL('0 0:45:00.000')),
       (3,
        2,
        1,
        'lol you''re just mad you lost :P',
        CURRENT_TIMESTAMP - TO_DSINTERVAL('0 0:15:00.000'));