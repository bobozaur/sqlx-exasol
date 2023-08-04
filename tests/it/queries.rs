#![cfg(feature = "migrate")]

use exasol::Exasol;
use sqlx::{pool::PoolConnection, query, Executor};
use sqlx_core::error::BoxDynError;

#[sqlx::test]
async fn test_comment(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute("-- This is a comment and should do nothing")
        .await?;
    Ok(())
}

#[sqlx::test]
async fn test_prepared_comment(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    query("-- This is a comment and should do nothing")
        .execute(&mut *con)
        .await?;

    Ok(())
}

#[sqlx::test]
async fn test_queries(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute("CREATE TABLE test_queries ( col VARCHAR(10) )")
        .await?;

    let res = con
        .execute("INSERT INTO test_queries VALUES ('test1')")
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("INSERT INTO test_queries VALUES (?)")
        .bind("test2")
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    let res = query("SELECT * FROM test_queries")
        .fetch_all(&mut *con)
        .await?;

    assert_eq!(res.len(), 2);

    let res = query("DELETE FROM test_queries WHERE col = ?")
        .bind("test2")
        .execute(&mut *con)
        .await?;

    assert_eq!(res.rows_affected(), 1);

    con.execute("DROP TABLE test_queries").await?;
    Ok(())
}
