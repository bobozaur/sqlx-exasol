use std::fmt::Write;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use futures_core::future::BoxFuture;

use sqlx_core::connection::Connection;

use sqlx_core::executor::Executor;
use sqlx_core::pool::{Pool, PoolOptions};
use sqlx_core::query::query;
use sqlx_core::query_builder::QueryBuilder;
use sqlx_core::query_scalar::query_scalar;
use sqlx_core::testing::*;
use sqlx_core::Error;

use crate::connection::ExaConnection;
use crate::database::Exasol;
use crate::options::ExaConnectOptions;

static MASTER_POOL: OnceLock<Pool<Exasol>> = OnceLock::new();
// Automatically delete any databases created before the start of the test binary.
static DO_CLEANUP: AtomicBool = AtomicBool::new(true);

impl TestSupport for Exasol {
    fn test_context(args: &TestArgs) -> BoxFuture<'_, Result<TestContext<Self>, Error>> {
        Box::pin(test_context(args))
    }

    fn cleanup_test(db_name: &str) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let mut conn = MASTER_POOL
                .get()
                .expect("cleanup_test() invoked outside `#[sqlx::test]")
                .acquire()
                .await?;

            let db_id = db_id(db_name);

            conn.execute(&format!("drop database if exists {};", db_name)[..])
                .await?;

            query(r#"DELETE FROM "_sqlx_test_databases" WHERE db_id = ?"#)
                .bind(db_id)
                .execute(&mut *conn)
                .await?;

            Ok(())
        })
    }

    fn cleanup_test_dbs() -> BoxFuture<'static, Result<Option<usize>, Error>> {
        Box::pin(async move {
            let url = dotenvy::var("DATABASE_URL").expect("DATABASE_URL must be set");

            let mut conn = ExaConnection::connect(&url).await?;

            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();

            let num_deleted = do_cleanup(&mut conn, now).await?;
            let _ = conn.close().await;
            Ok(Some(num_deleted))
        })
    }

    fn snapshot(
        _conn: &mut Self::Connection,
    ) -> BoxFuture<'_, Result<FixtureSnapshot<Self>, Error>> {
        // TODO: SQLx doesn't implement this yet either.
        todo!()
    }
}

async fn test_context(args: &TestArgs) -> Result<TestContext<Exasol>, Error> {
    let url = dotenvy::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let master_opts = ExaConnectOptions::from_str(&url).expect("failed to parse DATABASE_URL");

    let master_pool = MASTER_POOL.get_or_init(|| {
        PoolOptions::new()
            // Exasol supports 100 connections.
            // This should be more than enough for testing purposes.
            .max_connections(10)
            // Immediately close master connections. Tokio's I/O streams don't like hopping runtimes.
            .after_release(|_conn, _| Box::pin(async move { Ok(false) }))
            .connect_lazy_with(master_opts.clone())
    });

    // Sanity checks:
    assert_eq!(
        master_pool.connect_options().hosts,
        master_opts.hosts,
        "DATABASE_URL changed at runtime, host differs"
    );

    assert_eq!(
        master_pool.connect_options().schema,
        master_opts.schema,
        "DATABASE_URL changed at runtime, database differs"
    );

    let mut conn = master_pool.acquire().await?;

    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS "_sqlx_test_databases" (
            db_id BIGINT IDENTITY,
            test_path CLOB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    "#,
    )
    .await?;

    // Record the current time _before_ we acquire the `DO_CLEANUP` permit. This
    // prevents the first test thread from accidentally deleting new test dbs
    // created by other test threads if we're a bit slow.
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();

    // Only run cleanup if the test binary just started.
    if DO_CLEANUP.swap(false, Ordering::SeqCst) {
        do_cleanup(&mut conn, now).await?;
    }

    query(r#"INSERT INTO "_sqlx_test_databases" (test_path) VALUES (?)"#)
        .bind(args.test_path)
        .execute(&mut *conn)
        .await?;

    let new_db_id: u64 =
        query_scalar(r#"SELECT ZEROIFNULL(MAX(db_id)) + 1 FROM "_sqlx_test_databases";"#)
            .fetch_one(&mut *conn)
            .await?;

    let new_db_name = db_name(new_db_id);

    conn.execute(&format!("CREATE SCHEMA {}", new_db_name)[..])
        .await?;

    eprintln!("created database {}", new_db_name);

    let mut connect_opts = master_pool.connect_options().deref().clone();

    connect_opts.schema = Some(new_db_name.clone());

    Ok(TestContext {
        pool_opts: PoolOptions::new()
            // Don't allow a single test to take all the connections.
            // Most tests shouldn't require more than 5 connections concurrently,
            // or else they're likely doing too much in one test.
            .max_connections(5)
            // Close connections ASAP if left in the idle queue.
            .idle_timeout(Some(Duration::from_secs(1)))
            .parent(master_pool.clone()),
        connect_opts,
        db_name: new_db_name,
    })
}

async fn do_cleanup(conn: &mut ExaConnection, created_before: Duration) -> Result<usize, Error> {
    let delete_db_ids: Vec<u64> = query_scalar(
        r#"SELECT db_id FROM "_sqlx_test_databases" WHERE created_at < FROM_POSIX_TIME(?)"#,
    )
    .bind(created_before.as_secs())
    .fetch_all(&mut *conn)
    .await?;

    if delete_db_ids.is_empty() {
        return Ok(0);
    }

    let mut deleted_db_ids = Vec::with_capacity(delete_db_ids.len());

    let mut command = String::new();

    for db_id in delete_db_ids {
        command.clear();

        let db_name = db_name(db_id);

        writeln!(command, "DROP SCHEMA IF EXISTS {}", db_name).ok();
        match conn.execute(&*command).await {
            Ok(_deleted) => {
                deleted_db_ids.push(db_id);
            }
            // Assume a database error just means the DB is still in use.
            Err(Error::Database(dbe)) => {
                eprintln!("could not clean test database {:?}: {}", db_id, dbe)
            }
            // Bubble up other errors
            Err(e) => return Err(e),
        }
    }

    let mut query = QueryBuilder::new(r#"DELETE FROM "_sqlx_test_databases" WHERE db_id IN ("#);

    let mut separated = query.separated(",");

    for db_id in &deleted_db_ids {
        separated.push_bind(db_id);
    }

    query.push(")").build().execute(&mut *conn).await?;

    Ok(deleted_db_ids.len())
}

fn db_name(id: u64) -> String {
    format!(r#""_sqlx_test_database_{}""#, id)
}

fn db_id(name: &str) -> u64 {
    name.trim_start_matches(r#""_sqlx_test_database_"#)
        .trim_end_matches('"')
        .parse()
        .unwrap_or_else(|_1| panic!("failed to parse ID from database name {:?}", name))
}
