#![cfg(feature = "migrate")]

use std::iter::zip;

use futures_util::TryStreamExt;
use sqlx_exasol::{
    error::BoxDynError,
    pool::{PoolConnection, PoolOptions},
    AssertSqlSafe, Column, Connection, ExaConnectOptions, ExaConnection, ExaPool, ExaPoolOptions,
    ExaQueryResult, ExaRow, Exasol, Executor, Row, SqlStr, Statement, TypeInfo,
};

#[sqlx_exasol::test]
async fn it_connects(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    conn.ping().await?;
    conn.close().await?;

    Ok(())
}

#[sqlx_exasol::test]
async fn it_maths(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let value = sqlx_exasol::query("select 1 + CAST(? AS DECIMAL(5, 0))")
        .bind(5_i32.to_string())
        .try_map(|row: ExaRow| row.try_get::<i32, _>(0))
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(6i32, value);

    Ok(())
}

#[sqlx_exasol::test]
async fn it_can_fail_at_querying(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let _ = conn.execute(sqlx_exasol::query("SELECT 1")).await?;

    // we are testing that this does not cause a panic!
    let _ = conn
        .execute(sqlx_exasol::query("SELECT non_existence_table"))
        .await;

    Ok(())
}

#[sqlx_exasol::test]
async fn it_executes(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let _ = conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY);")
        .await?;

    for index in 1..=10_i32 {
        let done = sqlx_exasol::query("INSERT INTO users (id) VALUES (?)")
            .bind(index)
            .execute(&mut *conn)
            .await?;

        assert_eq!(done.rows_affected(), 1);
    }

    let sum: i64 = sqlx_exasol::query("SELECT id FROM users")
        .try_map(|row: ExaRow| row.try_get::<i64, _>(0))
        .fetch(&mut *conn)
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await?;

    assert_eq!(sum, 55);

    Ok(())
}

#[sqlx_exasol::test]
async fn it_executes_with_pool() -> anyhow::Result<()> {
    let pool: ExaPool = ExaPoolOptions::new()
        .min_connections(2)
        .max_connections(2)
        .test_before_acquire(false)
        .connect(&dotenvy::var("DATABASE_URL")?)
        .await?;

    let rows = pool.fetch_all("SELECT 1;").await?;

    assert_eq!(rows.len(), 1);

    let count = pool
        .fetch("SELECT 2;")
        .try_fold(0, |acc, _| async move { Ok(acc + 1) })
        .await?;

    assert_eq!(count, 1);

    Ok(())
}

#[sqlx_exasol::test]
async fn it_works_with_cache_disabled() -> anyhow::Result<()> {
    let mut url = url::Url::parse(&dotenvy::var("DATABASE_URL")?)?;
    url.query_pairs_mut()
        .append_pair("statement-cache-capacity", "1");

    let mut conn = ExaConnection::connect(url.as_ref()).await?;

    for index in 1..=10_i32 {
        let _ = sqlx_exasol::query("SELECT ?")
            .bind(index.to_string())
            .execute(&mut conn)
            .await?;
    }

    Ok(())
}

#[sqlx_exasol::test]
async fn it_drops_results_in_affected_rows(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    // ~1800 rows should be iterated and dropped
    let done = conn
        .execute("select * from EXA_TIME_ZONES limit 1800")
        .await?;

    // In Exasol, rows being returned isn't enough to flag it as an _affected_ row
    assert_eq!(0, done.rows_affected());

    Ok(())
}

#[sqlx_exasol::test]
async fn it_selects_null(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let (val,): (Option<i32>,) = sqlx_exasol::query_as("SELECT NULL")
        .fetch_one(&mut *conn)
        .await?;

    assert!(val.is_none());

    let val: Option<i32> = conn.fetch_one("SELECT NULL").await?.try_get(0)?;

    assert!(val.is_none());

    Ok(())
}

#[sqlx_exasol::test]
async fn it_can_fetch_one_and_ping(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let (_id,): (i32,) = sqlx_exasol::query_as("SELECT 1 as id")
        .fetch_one(&mut *conn)
        .await?;

    conn.ping().await?;

    let (_id,): (i32,) = sqlx_exasol::query_as("SELECT 1 as id")
        .fetch_one(&mut *conn)
        .await?;

    Ok(())
}

#[sqlx_exasol::test]
async fn it_caches_statements(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    for i in 0..2 {
        let row = sqlx_exasol::query("SELECT CAST(? as DECIMAL(5, 0)) AS val")
            .bind(i.to_string())
            .persistent(true)
            .fetch_one(&mut *conn)
            .await?;

        let val: i32 = row.get("val");

        assert_eq!(i, val);
    }

    assert_eq!(1, conn.cached_statements_size());
    conn.clear_cached_statements().await?;
    assert_eq!(0, conn.cached_statements_size());

    for i in 0..2 {
        let row = sqlx_exasol::query("SELECT CAST(? as DECIMAL(5, 0)) AS val")
            .bind(i.to_string())
            .persistent(false)
            .fetch_one(&mut *conn)
            .await?;

        let val: i32 = row.get("val");

        assert_eq!(i, val);
    }

    assert_eq!(0, conn.cached_statements_size());

    Ok(())
}

#[sqlx_exasol::test]
async fn it_can_bind_null_and_non_null_issue_540(
    mut conn: PoolConnection<Exasol>,
) -> anyhow::Result<()> {
    let row = sqlx_exasol::query("SELECT ?, ?, ?")
        .bind(50.to_string())
        .bind(None::<String>)
        .bind("")
        .fetch_one(&mut *conn)
        .await?;

    let v0: Option<String> = row.get(0);
    let v1: Option<String> = row.get(1);
    let v2: Option<String> = row.get(2);

    assert_eq!(v0, Some("50".to_owned()));
    assert_eq!(v1, None);
    assert_eq!(v2, None);

    Ok(())
}

#[sqlx_exasol::test]
async fn it_can_bind_only_null_issue_540(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let row = sqlx_exasol::query("SELECT ?")
        .bind(None::<String>)
        .fetch_one(&mut *conn)
        .await?;

    let v0: Option<String> = row.get(0);

    assert_eq!(v0, None);

    Ok(())
}

#[sqlx_exasol::test(migrations = "tests/setup")]
async fn it_can_prepare_then_execute(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let mut tx = conn.begin().await?;

    sqlx_exasol::query("INSERT INTO tweet ( text ) VALUES ( 'Hello, World' )")
        .execute(&mut *tx)
        .await?;

    let tweet_id: i64 = sqlx_exasol::query_scalar("SELECT id from tweet;")
        .fetch_one(&mut *tx)
        .await?;

    let statement = tx
        .prepare(SqlStr::from_static("SELECT * FROM tweet WHERE id = ?"))
        .await?;

    assert_eq!(statement.column(0).name(), "id");
    assert_eq!(statement.column(1).name(), "created_at");
    assert_eq!(statement.column(2).name(), "text");
    assert_eq!(statement.column(3).name(), "owner_id");

    assert_eq!(statement.column(0).type_info().name(), "DECIMAL(20, 0)");
    assert_eq!(statement.column(1).type_info().name(), "TIMESTAMP");
    assert_eq!(
        statement.column(2).type_info().name(),
        "VARCHAR(2000000) UTF8"
    );
    assert_eq!(statement.column(3).type_info().name(), "DECIMAL(20, 0)");

    let row = statement.query().bind(tweet_id).fetch_one(&mut *tx).await?;
    let tweet_text: &str = row.try_get("text")?;

    assert_eq!(tweet_text, "Hello, World");

    Ok(())
}

#[sqlx_exasol::test]
async fn it_can_work_with_transactions(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY);")
        .await?;

    // begin .. rollback

    let mut tx = conn.begin().await?;
    sqlx_exasol::query("INSERT INTO users (id) VALUES (?)")
        .bind(1_i32)
        .execute(&mut *tx)
        .await?;
    let count: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(count, 1);
    tx.rollback().await?;

    let count: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *conn)
        .await?;
    assert_eq!(count, 0);

    // begin .. commit

    let mut tx = conn.begin().await?;
    sqlx_exasol::query("INSERT INTO users (id) VALUES (?)")
        .bind(1_i32)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    let count: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *conn)
        .await?;
    assert_eq!(count, 1);

    // begin .. (drop)

    {
        let mut tx = conn.begin().await?;

        sqlx_exasol::query("INSERT INTO users (id) VALUES (?)")
            .bind(2)
            .execute(&mut *tx)
            .await?;

        let count: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM users")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(count, 2);
        // tx is dropped
    }

    let count: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *conn)
        .await?;
    assert_eq!(count, 1);

    Ok(())
}

#[sqlx_exasol::test]
async fn it_can_rollback_and_continue(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY);")
        .await?;

    // begin .. rollback

    let mut tx = conn.begin().await?;
    sqlx_exasol::query("INSERT INTO users (id) VALUES (?)")
        .bind(vec![1, 2])
        .execute(&mut *tx)
        .await?;
    let count: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(count, 2);
    tx.rollback().await?;

    sqlx_exasol::query("INSERT INTO users (id) VALUES (?)")
        .bind(1)
        .execute(&mut *conn)
        .await?;

    let count: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *conn)
        .await?;
    assert_eq!(count, 1);

    Ok(())
}

#[sqlx_exasol::test]
async fn it_cannot_nest_transactions(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let mut tx = conn.begin().await?;
    // Trying to start a nested one will fail.
    assert!(tx.begin().await.is_err());

    Ok(())
}

// This cannot unfortunately be achieved without some async drop.
//
// We can either schedule the rollback to be sent on the next async database interaction or
// pre-emptively start sending the message, but in this case unless we get to flush it, we're still
// not going to be getting a response until we have some other database interaction.
//
// Therefore, if a transaction is dropped and scheduled for rollback but another connection starts a
// conflicting transaction, a deadlock will occur.
//
// #[sqlx_exasol::test]
// async fn it_can_drop_transaction_and_not_deadlock(
//     pool_opts: PoolOptions<Exasol>,
//     exa_opts: ExaConnectOptions,
// ) -> anyhow::Result<()> {
//     let pool_opts = pool_opts.max_connections(2);
//     let pool = pool_opts.connect_with(exa_opts).await?;
//     let mut conn1 = pool.acquire().await?;
//     let mut conn2 = pool.acquire().await?;

//     conn1
//         .execute("CREATE TABLE users (id INTEGER PRIMARY KEY);")
//         .await?;

//     // begin .. drop

//     {
//         let mut tx = conn1.begin().await?;
//         sqlx_exasol::query("INSERT INTO users (id) VALUES (?)")
//             .bind(vec![1, 2])
//             .execute(&mut *tx)
//             .await?;
//         let count: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM users")
//             .fetch_one(&mut *tx)
//             .await?;
//         assert_eq!(count, 2);

//         sqlx_exasol::query("INSERT INTO users (id) VALUES (?)")
//             .bind(vec![3, 4])
//             .execute(&mut *tx)
//             .await?;
//     }

//     sqlx_exasol::query("INSERT INTO users (id) VALUES (?)")
//         .bind(5)
//         .execute(&mut *conn2)
//         .await?;

//     let count: i64 = sqlx_exasol::query_scalar("SELECT COUNT(*) FROM users")
//         .fetch_one(&mut *conn2)
//         .await?;
//     assert_eq!(count, 1);

//     Ok(())
// }

#[sqlx_exasol::test]
async fn test_equal_arrays(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute(
        "CREATE TABLE sqlx_test_type ( col1 BOOLEAN, col2 DECIMAL(10, 0), col3 VARCHAR(100) );",
    )
    .await?;

    let bools = vec![false, true, false];
    let ints = vec![1, 2, 3];
    let mut strings = vec![Some("one".to_owned()), None, Some(String::new())];

    let query_result = sqlx_exasol::query("INSERT INTO sqlx_test_type VALUES (?, ?, ?)")
        .bind(&bools)
        .bind(&ints)
        .bind(&strings)
        .execute(&mut *con)
        .await?;

    assert_eq!(query_result.rows_affected(), 3);

    let values: Vec<(bool, i32, Option<String>)> =
        sqlx_exasol::query_as("SELECT * FROM sqlx_test_type ORDER BY col2;")
            .fetch_all(&mut *con)
            .await?;

    // Exasol treats empty strings as NULL
    strings.pop();
    strings.push(None);

    let expected = zip(zip(bools, ints), strings).map(|((b, i), s)| (b, i, s));
    for (v, e) in zip(values, expected) {
        assert_eq!(v, e);
    }

    Ok(())
}

#[sqlx_exasol::test]
async fn test_unequal_arrays(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute(
        "CREATE TABLE sqlx_test_type ( col1 BOOLEAN, col2 DECIMAL(10, 0), col3 VARCHAR(100) );",
    )
    .await?;

    let bools = vec![false, true, false];
    let ints = vec![1, 2, 3, 4];
    let strings = vec![Some("one".to_owned()), Some(String::new())];

    sqlx_exasol::query("INSERT INTO sqlx_test_type VALUES (?, ?, ?)")
        .bind(&bools)
        .bind(&ints)
        .bind(&strings)
        .execute(&mut *con)
        .await
        .unwrap_err();

    Ok(())
}

#[sqlx_exasol::test]
async fn test_exceeding_arrays(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute(
        "CREATE TABLE sqlx_test_type ( col1 BOOLEAN, col2 DECIMAL(10, 0), col3 VARCHAR(100) );",
    )
    .await?;

    let bools = vec![false, true, false];
    let ints = vec![1, 2, i64::MAX];
    let strings = vec![Some("one".to_owned()), Some(String::new()), None];

    sqlx_exasol::query("INSERT INTO sqlx_test_type VALUES (?, ?, ?)")
        .bind(&bools)
        .bind(&ints)
        .bind(&strings)
        .execute(&mut *con)
        .await
        .unwrap_err();

    Ok(())
}

#[sqlx_exasol::test]
async fn test_decode_error(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute("CREATE TABLE sqlx_test_type ( col DECIMAL(10, 0) );")
        .await?;

    sqlx_exasol::query("INSERT INTO sqlx_test_type VALUES (?)")
        .bind(i32::MAX)
        .execute(&mut *con)
        .await?;

    let error = sqlx_exasol::query_scalar::<_, i8>("SELECT col FROM sqlx_test_type")
        .fetch_one(&mut *con)
        .await
        .unwrap_err();

    eprintln!("{error}");

    Ok(())
}

#[sqlx_exasol::test]
async fn test_execute_many_works(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    con.execute_many("SELECT 1; SELECT 2; SELECT 3;")
        .try_collect::<ExaQueryResult>()
        .await?;

    Ok(())
}

/// Ensure that even if errors are not handled a bad statement in a query will still result in the
/// stream ending.
#[sqlx_exasol::test]
async fn test_execute_many_fails_bad_query(
    mut con: PoolConnection<Exasol>,
) -> Result<(), BoxDynError> {
    let res = con
        .execute_many("SELECT 1; SELECT * FROM some_table_that_does_not_exist; SELECT 2;")
        .try_collect::<ExaQueryResult>()
        .await;

    assert!(res.is_err());

    Ok(())
}

#[expect(deprecated, reason = "testing deprecation")]
#[sqlx_exasol::test]
async fn test_execute_many_fails_params(
    mut con: PoolConnection<Exasol>,
) -> Result<(), BoxDynError> {
    // Fails because this is a multi-statement query.
    let is_err = sqlx_exasol::query("SELECT ?; SELECT ?")
        .bind(1)
        .bind(2)
        .execute_many(&mut *con)
        .await
        .try_collect::<ExaQueryResult>()
        .await
        .is_err();

    assert!(is_err);

    Ok(())
}

#[sqlx_exasol::test]
async fn test_fetch_many_works(mut con: PoolConnection<Exasol>) -> Result<(), BoxDynError> {
    let mut stream = con.fetch_many(
        "
        CREATE TABLE FETCH_TEST (col VARCHAR(20));
        INSERT INTO FETCH_TEST VALUES ('test');
        SELECT * FROM FETCH_TEST;",
    );

    while stream.try_next().await?.is_some() {}

    Ok(())
}

#[sqlx_exasol::test]
async fn it_works_on_large_datasets(mut con: PoolConnection<Exasol>) -> anyhow::Result<()> {
    sqlx::query("CREATE TABLE large_dataset (col1 VARCHAR(20), col2 VARCHAR(20));")
        .execute(&mut *con)
        .await?;

    let data = vec!["test"; 100_000];

    for _ in 0..50 {
        sqlx::query("INSERT INTO large_dataset VALUES(?, ?);")
            .bind(&data)
            .bind(&data)
            .execute(&mut *con)
            .await?;
    }

    let mut rows = sqlx::query("SELECT col1, col2 FROM large_dataset").fetch(&mut *con);

    while let Some(row_result) = rows.try_next().await? {
        let row = row_result;
        let _: String = row.get("col1");
        let _: String = row.get("col2");
    }
    Ok(())
}

#[sqlx_exasol::test]
async fn it_selects_schema(
    pool_opts: PoolOptions<Exasol>,
    exa_opts: ExaConnectOptions,
) -> Result<(), BoxDynError> {
    let pool = pool_opts.connect_with(exa_opts).await?;
    let mut con = pool.acquire().await?;

    let schema: Option<String> = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
        .fetch_one(&mut *con)
        .await?;

    assert!(schema.is_some());

    Ok(())
}

#[sqlx_exasol::test]
async fn it_switches_schema(
    pool_opts: PoolOptions<Exasol>,
    exa_opts: ExaConnectOptions,
) -> Result<(), BoxDynError> {
    let pool = pool_opts.connect_with(exa_opts).await?;
    let mut con = pool.acquire().await?;
    let schema = "TEST_SWITCH_SCHEMA";

    con.execute(AssertSqlSafe(
        format!("CREATE SCHEMA IF NOT EXISTS {schema};").as_str(),
    ))
    .await?;

    let new_schema: String = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
        .fetch_one(&mut *con)
        .await?;

    con.execute(AssertSqlSafe(
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE;").as_str(),
    ))
    .await?;

    assert_eq!(schema, new_schema);

    Ok(())
}

#[sqlx_exasol::test]
async fn it_switches_schema_from_attr(
    pool_opts: PoolOptions<Exasol>,
    exa_opts: ExaConnectOptions,
) -> Result<(), BoxDynError> {
    let pool = pool_opts.connect_with(exa_opts).await?;
    let mut con = pool.acquire().await?;

    let orig_schema: String = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
        .fetch_one(&mut *con)
        .await?;

    let schema = "TEST_SWITCH_SCHEMA_ATTR";

    con.execute(AssertSqlSafe(
        format!("CREATE SCHEMA IF NOT EXISTS {schema};").as_str(),
    ))
    .await?;

    con.attributes_mut().set_current_schema(orig_schema.clone());
    con.flush_attributes().await?;

    let new_schema: String = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
        .fetch_one(&mut *con)
        .await?;

    assert_eq!(orig_schema, new_schema);

    Ok(())
}

#[sqlx_exasol::test]
async fn it_closes_schema_and_returns_none(
    pool_opts: PoolOptions<Exasol>,
    exa_opts: ExaConnectOptions,
) -> Result<(), BoxDynError> {
    let pool = pool_opts.connect_with(exa_opts).await?;
    let mut con = pool.acquire().await?;

    let orig_schema: String = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
        .fetch_one(&mut *con)
        .await?;

    assert_eq!(
        con.attributes().current_schema(),
        Some(orig_schema.as_str())
    );

    con.execute("CLOSE SCHEMA").await?;
    assert_eq!(con.attributes().current_schema(), None);

    Ok(())
}

#[sqlx_exasol::test]
async fn it_accepts_comment_only_stmt(
    pool_opts: PoolOptions<Exasol>,
    exa_opts: ExaConnectOptions,
) -> Result<(), BoxDynError> {
    let pool = pool_opts.connect_with(exa_opts).await?;
    let mut con = pool.acquire().await?;

    con.execute_many("/* this is a comment */")
        .try_collect::<ExaQueryResult>()
        .await?;
    con.execute("-- this is a comment").await?;

    Ok(())
}

#[sqlx_exasol::test]
async fn it_flushes_on_drop(
    pool_opts: PoolOptions<Exasol>,
    exa_opts: ExaConnectOptions,
) -> Result<(), BoxDynError> {
    // Only allow one connection
    let pool = pool_opts.max_connections(1).connect_with(exa_opts).await?;
    pool.execute("CREATE TABLE TRANSACTIONS_TEST ( col DECIMAL(1, 0) );")
        .await?;

    {
        let mut conn = pool.acquire().await?;
        let mut tx = conn.begin().await?;
        tx.execute("INSERT INTO TRANSACTIONS_TEST VALUES(1)")
            .await?;
    }

    let mut conn = pool.acquire().await?;
    {
        let mut tx = conn.begin().await?;
        tx.execute("INSERT INTO TRANSACTIONS_TEST VALUES(1)")
            .await?;
    }

    {
        let mut tx = conn.begin().await?;
        tx.execute("INSERT INTO TRANSACTIONS_TEST VALUES(1)")
            .await?;
    }

    drop(conn);

    let inserted = pool
        .fetch_all("SELECT * FROM TRANSACTIONS_TEST")
        .await?
        .len();

    assert_eq!(inserted, 0);
    Ok(())
}
