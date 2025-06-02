use std::{ops::Deref, str::FromStr, sync::OnceLock, time::Duration};

use futures_core::future::BoxFuture;
use futures_util::TryStreamExt;
use sqlx_core::{
    connection::Connection,
    error::DatabaseError,
    executor::Executor,
    pool::{Pool, PoolOptions},
    query, query_scalar,
    testing::{FixtureSnapshot, TestArgs, TestContext, TestSupport},
    Error,
};

use crate::{
    connection::ExaConnection, database::Exasol, options::ExaConnectOptions, ExaQueryResult,
};

static MASTER_POOL: OnceLock<Pool<Exasol>> = OnceLock::new();

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

            do_cleanup(&mut conn, db_name).await
        })
    }

    fn cleanup_test_dbs() -> BoxFuture<'static, Result<Option<usize>, Error>> {
        Box::pin(async move {
            let url = dotenvy::var("DATABASE_URL").expect("DATABASE_URL must be set");

            let mut conn = ExaConnection::connect(&url).await?;

            let query_str = r#"SELECT db_name FROM "_sqlx_tests"."_sqlx_test_databases";"#;
            let db_names_to_delete: Vec<String> = query_scalar::query_scalar(query_str)
                .fetch_all(&mut conn)
                .await?;

            if db_names_to_delete.is_empty() {
                return Ok(None);
            }

            let mut deleted_db_names = Vec::with_capacity(db_names_to_delete.len());

            for db_name in &db_names_to_delete {
                let query_str = format!(r#"DROP SCHEMA IF EXISTS "{db_name}" CASCADE;"#);

                match conn.execute(&*query_str).await {
                    Ok(_deleted) => {
                        deleted_db_names.push(db_name);
                    }
                    // Assume a database error just means the DB is still in use.
                    Err(Error::Database(dbe)) => {
                        eprintln!("could not clean test database {db_name}: {dbe}");
                    }
                    // Bubble up other errors
                    Err(e) => return Err(e),
                }
            }

            if deleted_db_names.is_empty() {
                return Ok(None);
            }

            query::query(r#"DELETE FROM "_sqlx_tests"."_sqlx_test_databases" WHERE db_name = ?;"#)
                .bind(&deleted_db_names)
                .execute(&mut conn)
                .await?;

            conn.close().await.ok();
            Ok(Some(db_names_to_delete.len()))
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

    cleanup_old_dbs(&mut conn).await?;

    let setup_res = conn
        .execute_many(
            r#"
        CREATE SCHEMA IF NOT EXISTS "_sqlx_tests";
        CREATE TABLE IF NOT EXISTS "_sqlx_tests"."_sqlx_test_databases" (
            db_name CLOB NOT NULL,
            test_path CLOB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"#,
        )
        .try_collect::<ExaQueryResult>()
        .await;

    if let Err(e) = setup_res {
        match e
            .as_database_error()
            .and_then(DatabaseError::code)
            .as_deref()
        {
            // Error code for when an object already exists.
            //
            // Multiple tests concurrenclty trying to create the test schema and table can cause a
            // `GlobalTransactionRollback`, where the objects did not exist when creating them was
            // attempted but they got created by another test before the current one could create
            // them. This means that the objects now exist, which is what we wanted all along.
            Some("40001") => Ok(()),
            _ => Err(e),
        }?;
    }

    let db_name = Exasol::db_name(args);
    do_cleanup(&mut conn, &db_name).await?;

    let mut tx = conn.begin().await?;

    let query_str = r#"
        INSERT INTO "_sqlx_tests"."_sqlx_test_databases" (db_name, test_path)
        VALUES (?, ?)"#;

    query::query(query_str)
        .bind(&db_name)
        .bind(args.test_path)
        .execute(&mut *tx)
        .await?;

    tx.execute(&*format!(r#"CREATE SCHEMA "{db_name}";"#))
        .await?;
    tx.commit().await?;

    eprintln!("created database {db_name}");

    let mut connect_opts = master_pool.connect_options().deref().clone();

    connect_opts.schema = Some(db_name.clone());

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
        db_name,
    })
}

async fn do_cleanup(conn: &mut ExaConnection, db_name: &str) -> Result<(), Error> {
    conn.execute(&*format!(r#"DROP SCHEMA IF EXISTS "{db_name}" CASCADE"#))
        .await?;

    query::query(r#"DELETE FROM "_sqlx_tests"."_sqlx_test_databases" WHERE db_name = ?;"#)
        .bind(db_name)
        .execute(&mut *conn)
        .await?;

    Ok(())
}

/// Pre <0.8.4, test databases were stored by integer ID.
async fn cleanup_old_dbs(conn: &mut ExaConnection) -> Result<(), Error> {
    let res =
        query_scalar::query_scalar(r#"SELECT db_id FROM "_sqlx_tests"."_sqlx_test_databases";"#)
            .fetch_all(&mut *conn)
            .await;

    let db_ids: Vec<u64> = match res {
        Ok(db_ids) => db_ids,
        Err(e) => {
            return match e
                .as_database_error()
                .and_then(DatabaseError::code)
                .as_deref()
            {
                // Common error code for when an object does not exist.
                //
                // Applies to both a missing `_sqlx_test_databases` table,
                // in which case no cleanup is needed OR a missing `db_id`
                // column, in which case the table has already been migrated.
                Some("42000") => Ok(()),
                _ => Err(e),
            };
        }
    };

    // Drop old-style test databases.
    for id in db_ids {
        let query = format!(r#"DROP SCHEMA IF EXISTS "_sqlx_test_database_{id}" CASCADE"#);
        match conn.execute(&*query).await {
            Ok(_deleted) => (),
            // Assume a database error just means the DB is still in use.
            Err(Error::Database(dbe)) => {
                eprintln!("could not clean old test database _sqlx_test_database_{id}: {dbe}");
            }
            // Bubble up other errors
            Err(e) => return Err(e),
        }
    }

    conn.execute(r#"DROP TABLE IF EXISTS "_sqlx_tests"."_sqlx_test_databases";"#)
        .await?;

    Ok(())
}
