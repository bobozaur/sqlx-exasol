#![cfg(feature = "migrate")]

use sqlx_exasol::{error::ErrorKind, pool::PoolConnection, Exasol};

#[sqlx_exasol::test(migrations = "tests/setup")]
async fn it_fails_with_unique_violation(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    sqlx_exasol::query("INSERT INTO tweet(id, text, owner_id) VALUES (1, 'Foo', 1)")
        .execute(&mut *conn)
        .await?;

    let res: Result<_, sqlx_exasol::Error> =
        sqlx_exasol::query("INSERT INTO tweet VALUES (1, NOW(), 'Foo', 1);")
            .execute(&mut *conn)
            .await;

    let err = res.unwrap_err();

    let err = err.into_database_error().unwrap();

    assert_eq!(err.kind(), ErrorKind::UniqueViolation);

    Ok(())
}

#[sqlx_exasol::test(migrations = "tests/setup")]
async fn it_fails_with_foreign_key_violation(
    mut conn: PoolConnection<Exasol>,
) -> anyhow::Result<()> {
    let res: Result<_, sqlx_exasol::Error> =
        sqlx_exasol::query("INSERT INTO tweet_reply (tweet_id, text) VALUES (1, 'Reply!');")
            .execute(&mut *conn)
            .await;

    let err = res.unwrap_err();

    let err = err.into_database_error().unwrap();

    assert_eq!(err.kind(), ErrorKind::ForeignKeyViolation);

    Ok(())
}

#[sqlx_exasol::test(migrations = "tests/setup")]
async fn it_fails_with_not_null_violation(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let res: Result<_, sqlx_exasol::Error> =
        sqlx_exasol::query("INSERT INTO tweet (text) VALUES (null);")
            .execute(&mut *conn)
            .await;
    let err = res.unwrap_err();

    let err = err.into_database_error().unwrap();

    assert_eq!(err.kind(), ErrorKind::NotNullViolation);

    Ok(())
}
