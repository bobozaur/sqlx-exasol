use std::path::Path;

use sqlx::{migrate::Migrator, pool::PoolConnection, Executor, Row};
use sqlx_exasol::{ExaConnection, Exasol};

#[sqlx::test(migrations = false)]
async fn simple(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    clean_up(&mut conn).await?;

    let migrator = Migrator::new(Path::new("tests/it/migrations_simple")).await?;

    // run migration
    migrator.run(&mut conn).await?;

    // check outcome
    let res: String = conn
        .fetch_one("SELECT some_payload FROM migrations_simple_test")
        .await?
        .get(0);
    assert_eq!(res, "110_suffix");

    // running it a 2nd time should still work
    migrator.run(&mut conn).await?;

    Ok(())
}

#[sqlx::test(migrations = false)]
async fn reversible(mut conn: PoolConnection<Exasol>) -> anyhow::Result<()> {
    clean_up(&mut conn).await?;

    let migrator = Migrator::new(Path::new("tests/it/migrations_reversible")).await?;

    // run migration
    migrator.run(&mut conn).await?;

    // check outcome
    let res: i64 = conn
        .fetch_one("SELECT some_payload FROM migrations_reversible_test")
        .await?
        .get(0);
    assert_eq!(res, 101);

    // roll back nothing (last version)
    migrator.undo(&mut conn, 2).await?;

    // check outcome
    let res: i64 = conn
        .fetch_one("SELECT some_payload FROM migrations_reversible_test")
        .await?
        .get(0);
    assert_eq!(res, 101);

    // roll back one version
    migrator.undo(&mut conn, 1).await?;

    // check outcome
    let res: i64 = conn
        .fetch_one("SELECT some_payload FROM migrations_reversible_test")
        .await?
        .get(0);
    assert_eq!(res, 100);

    Ok(())
}

/// Ensure that we have a clean initial state.
async fn clean_up(conn: &mut ExaConnection) -> anyhow::Result<()> {
    conn.execute("DROP TABLE migrations_simple_test").await.ok();
    conn.execute("DROP TABLE migrations_reversible_test")
        .await
        .ok();
    conn.execute("DROP TABLE _sqlx_migrations").await.ok();

    Ok(())
}
