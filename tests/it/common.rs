use exasol::{ExaConnection, ExaPool, ExaPoolOptions, ExaRow, Exasol};
use futures_util::TryStreamExt;
use sqlx::{Column, Connection, Executor, Row, Statement, TypeInfo};
use sqlx_core::pool::PoolConnection;

#[sqlx::test]
async fn it_connects(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    conn.ping().await?;
    conn.close().await?;

    Ok(())
}

#[sqlx::test]
async fn it_maths(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let value = sqlx::query("select 1 + CAST(? AS DECIMAL(5, 0))")
        .bind(5_i32.to_string())
        .try_map(|row: ExaRow| row.try_get::<i32, _>(0))
        .fetch_one(&mut *conn)
        .await?;

    assert_eq!(6i32, value);

    Ok(())
}

#[sqlx::test]
async fn it_can_fail_at_querying(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let _ = conn.execute(sqlx::query("SELECT 1")).await?;

    // we are testing that this does not cause a panic!
    let _ = conn
        .execute(sqlx::query("SELECT non_existence_table"))
        .await;

    Ok(())
}

#[sqlx::test]
async fn it_executes(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let _ = conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY);")
        .await?;

    for index in 1..=10_i32 {
        let done = sqlx::query("INSERT INTO users (id) VALUES (?)")
            .bind(index)
            .execute(&mut *conn)
            .await?;

        assert_eq!(done.rows_affected(), 1);
    }

    let sum: i64 = sqlx::query("SELECT id FROM users")
        .try_map(|row: ExaRow| row.try_get::<i64, _>(0))
        .fetch(&mut *conn)
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await?;

    assert_eq!(sum, 55);

    Ok(())
}

#[sqlx::test]
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

#[sqlx::test]
async fn it_works_with_cache_disabled() -> anyhow::Result<()> {
    let mut url = url::Url::parse(&dotenvy::var("DATABASE_URL")?)?;
    url.query_pairs_mut()
        .append_pair("statement-cache-capacity", "1");

    let mut conn = ExaConnection::connect(url.as_ref()).await?;

    for index in 1..=10_i32 {
        let _ = sqlx::query("SELECT ?")
            .bind(index.to_string())
            .execute(&mut conn)
            .await?;
    }

    Ok(())
}

#[sqlx::test]
async fn it_drops_results_in_affected_rows(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    // ~1800 rows should be iterated and dropped
    let done = conn
        .execute("select * from EXA_TIME_ZONES limit 1800")
        .await?;

    // In Exasol, rows being returned isn't enough to flag it as an _affected_ row
    assert_eq!(0, done.rows_affected());

    Ok(())
}

#[sqlx::test]
async fn it_selects_null(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let (val,): (Option<i32>,) = sqlx::query_as("SELECT NULL").fetch_one(&mut *conn).await?;

    assert!(val.is_none());

    let val: Option<i32> = conn.fetch_one("SELECT NULL").await?.try_get(0)?;

    assert!(val.is_none());

    Ok(())
}

#[sqlx::test]
async fn it_can_fetch_one_and_ping(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let (_id,): (i32,) = sqlx::query_as("SELECT 1 as id")
        .fetch_one(&mut *conn)
        .await?;

    conn.ping().await?;

    let (_id,): (i32,) = sqlx::query_as("SELECT 1 as id")
        .fetch_one(&mut *conn)
        .await?;

    Ok(())
}

#[sqlx::test]
async fn it_caches_statements(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    for i in 0..2 {
        let row = sqlx::query("SELECT CAST(? as DECIMAL(5, 0)) AS val")
            .bind(i.to_string())
            .persistent(true)
            .fetch_one(&mut *conn)
            .await?;

        let val: u32 = row.get("val");

        assert_eq!(i, val);
    }

    assert_eq!(1, conn.cached_statements_size());
    conn.clear_cached_statements().await?;
    assert_eq!(0, conn.cached_statements_size());

    for i in 0..2 {
        let row = sqlx::query("SELECT CAST(? as DECIMAL(5, 0)) AS val")
            .bind(i.to_string())
            .persistent(false)
            .fetch_one(&mut *conn)
            .await?;

        let val: u32 = row.get("val");

        assert_eq!(i, val);
    }

    assert_eq!(0, conn.cached_statements_size());

    Ok(())
}

#[sqlx::test]
async fn it_can_bind_null_and_non_null_issue_540(
    mut conn: PoolConnection<Exasol>,
) -> anyhow::Result<()> {
    let row = sqlx::query("SELECT ?, ?, ?")
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

#[sqlx::test]
async fn it_can_bind_only_null_issue_540(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let row = sqlx::query("SELECT ?")
        .bind(None::<i32>)
        .fetch_one(&mut *conn)
        .await?;

    let v0: Option<i32> = row.get(0);

    assert_eq!(v0, None);

    Ok(())
}

#[sqlx::test(migrations = "tests/it/setup")]
async fn it_can_prepare_then_execute(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    let mut tx = conn.begin().await?;

    sqlx::query("INSERT INTO tweet ( text ) VALUES ( 'Hello, World' )")
        .execute(&mut *tx)
        .await?;

    let tweet_id: u64 = sqlx::query_scalar("SELECT id from tweet;")
        .fetch_one(&mut *tx)
        .await?;

    let statement = tx.prepare("SELECT * FROM tweet WHERE id = ?").await?;

    assert_eq!(statement.column(0).name(), "id");
    assert_eq!(statement.column(1).name(), "created_at");
    assert_eq!(statement.column(2).name(), "text");
    assert_eq!(statement.column(3).name(), "owner_id");

    assert_eq!(statement.column(0).type_info().name(), "DECIMAL");
    assert_eq!(statement.column(1).type_info().name(), "TIMESTAMP");
    assert_eq!(statement.column(2).type_info().name(), "VARCHAR");
    assert_eq!(statement.column(3).type_info().name(), "DECIMAL");

    let row = statement.query().bind(tweet_id).fetch_one(&mut *tx).await?;
    let tweet_text: &str = row.try_get("text")?;

    assert_eq!(tweet_text, "Hello, World");

    Ok(())
}

#[sqlx::test]
async fn it_can_work_with_transactions(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY);")
        .await?;

    // begin .. rollback

    let mut tx = conn.begin().await?;
    sqlx::query("INSERT INTO users (id) VALUES (?)")
        .bind(1_i32)
        .execute(&mut *tx)
        .await?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(count, 1);
    tx.rollback().await?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *conn)
        .await?;
    assert_eq!(count, 0);

    // begin .. commit

    let mut tx = conn.begin().await?;
    sqlx::query("INSERT INTO users (id) VALUES (?)")
        .bind(1_i32)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *conn)
        .await?;
    assert_eq!(count, 1);

    // begin .. (drop)

    {
        let mut tx = conn.begin().await?;

        sqlx::query("INSERT INTO users (id) VALUES (?)")
            .bind(2)
            .execute(&mut *tx)
            .await?;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(count, 2);
        // tx is dropped
    }

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *conn)
        .await?;
    assert_eq!(count, 1);

    Ok(())
}

#[sqlx::test]
async fn it_can_rollback_and_continue(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY);")
        .await?;

    // begin .. rollback

    let mut tx = conn.begin().await?;
    sqlx::query("INSERT INTO users (id) VALUES (?)")
        .bind(vec![1, 2])
        .execute(&mut *tx)
        .await?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(count, 2);
    tx.rollback().await?;

    sqlx::query("INSERT INTO users (id) VALUES (?)")
        .bind(1)
        .execute(&mut *conn)
        .await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *conn)
        .await?;
    assert_eq!(count, 1);

    Ok(())
}
