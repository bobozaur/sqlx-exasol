use std::{
    fmt::Write,
    ops::Deref,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        OnceLock,
    },
    time::{Duration, SystemTime},
};

use futures_core::future::BoxFuture;
use sqlx_core::{
    connection::Connection,
    executor::Executor,
    pool::{Pool, PoolOptions},
    query::{self},
    query_scalar::query_scalar,
    testing::{FixtureSnapshot, TestArgs, TestContext, TestSupport},
    Error,
};

use crate::{
    connection::{
        futures::{ExecuteBatch, WebSocketFuture},
        ExaConnection,
    },
    database::Exasol,
    options::ExaConnectOptions,
};

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

            let query_str = format!("DROP SCHEMA IF EXISTS {db_name} CASCADE;");
            conn.execute(&*query_str).await?;

            let query_str = r#"DELETE FROM "_sqlx_tests"."_sqlx_test_databases" WHERE db_id = ?;"#;
            query::query(query_str)
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
            .max_connections(20)
            // Immediately close master connections. Tokio's I/O streams don't like hopping
            // runtimes.
            .after_release(|_conn, _| Box::pin(async move { Ok(false) }))
            .connect_lazy_with(master_opts.clone())
    });

    // Sanity checks:
    assert_eq!(
        master_pool.connect_options().hosts_details,
        master_opts.hosts_details,
        "DATABASE_URL changed at runtime, host differs"
    );

    assert_eq!(
        master_pool.connect_options().schema,
        master_opts.schema,
        "DATABASE_URL changed at runtime, database differs"
    );

    let mut conn = master_pool.acquire().await?;

    let queries = r#"CREATE SCHEMA IF NOT EXISTS "_sqlx_tests";
        "CREATE TABLE IF NOT EXISTS "_sqlx_tests"."_sqlx_test_databases" (
            db_id DECIMAL(20, 0) IDENTITY,
            test_path CLOB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"#;

    ExecuteBatch::new(queries).future(&mut conn.ws).await?;

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

    let mut tx = conn.begin().await?;

    let query_str = r#"INSERT INTO "_sqlx_tests"."_sqlx_test_databases" (test_path) VALUES (?)"#;
    query::query(query_str)
        .bind(args.test_path)
        .execute(&mut *tx)
        .await?;

    let query_str = r#"SELECT MAX(db_id) FROM "_sqlx_tests"."_sqlx_test_databases";"#;
    let new_db_id: u64 = query_scalar(query_str).fetch_one(&mut *tx).await?;
    let new_db_name = db_name(new_db_id);

    let query_str = format!("CREATE SCHEMA {new_db_name}");
    tx.execute(&*query_str).await?;
    tx.commit().await?;

    eprintln!("created database {new_db_name}");

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
    let query_str = r#"
        SELECT db_id FROM
        "_sqlx_tests"."_sqlx_test_databases"
        WHERE created_at < FROM_POSIX_TIME(?);
        "#;

    let ids_to_delete: Vec<u64> = query_scalar(query_str)
        .bind(created_before.as_secs().to_string())
        .fetch_all(&mut *conn)
        .await?;

    if ids_to_delete.is_empty() {
        return Ok(0);
    }

    let mut deleted_db_ids = Vec::with_capacity(ids_to_delete.len());

    let mut command = String::new();

    for db_id in ids_to_delete {
        command.clear();

        let db_name = db_name(db_id);

        writeln!(command, "DROP SCHEMA IF EXISTS {db_name} CASCADE").ok();
        match conn.execute(&*command).await {
            Ok(_deleted) => {
                deleted_db_ids.push(db_id);
            }
            // Assume a database error just means the DB is still in use.
            Err(Error::Database(dbe)) => {
                eprintln!("could not clean test database {db_id:?}: {dbe}");
            }
            // Bubble up other errors
            Err(e) => return Err(e),
        }
    }

    query::query(r#"DELETE FROM "_sqlx_tests"."_sqlx_test_databases" WHERE db_id = ?;"#)
        .bind(&deleted_db_ids)
        .execute(&mut *conn)
        .await?;

    Ok(deleted_db_ids.len())
}

fn db_name(id: u64) -> String {
    format!(r#""_sqlx_test_database_{id}""#)
}

fn db_id(name: &str) -> u64 {
    name.trim_start_matches(r#""_sqlx_test_database_"#)
        .trim_end_matches('"')
        .parse()
        .unwrap_or_else(|_1| panic!("failed to parse ID from database name {name:?}"))
}

#[test]
fn test_db_name_id() {
    assert_eq!(db_name(12345), r#""_sqlx_test_database_12345""#);
    assert_eq!(db_id(r#""_sqlx_test_database_12345""#), 12345);
}
