use std::str::FromStr;
use std::time::Duration;
use std::time::Instant;

use futures_core::future::BoxFuture;

use sqlx_core::connection::ConnectOptions;
use sqlx_core::connection::Connection;
use sqlx_core::executor::Executor;
use sqlx_core::migrate::MigrateError;
use sqlx_core::migrate::{AppliedMigration, Migration};
use sqlx_core::migrate::{Migrate, MigrateDatabase};
use sqlx_core::query::query;
use sqlx_core::query_as::query_as;
use sqlx_core::query_scalar::query_scalar;
use sqlx_core::Error;

use crate::connection::ExaConnection;
use crate::database::Exasol;
use crate::options::ExaConnectOptions;

fn parse_for_maintenance(url: &str) -> Result<(ExaConnectOptions, String), Error> {
    let mut options = ExaConnectOptions::from_str(url)?;

    let database = options
        .schema
        .ok_or_else(|| Error::Configuration("DATABASE_URL does not specify a database".into()))?;

    // switch to <no> database for create/drop commands
    options.schema = None;

    Ok((options, database))
}

impl MigrateDatabase for Exasol {
    fn create_database(url: &str) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let (options, database) = parse_for_maintenance(url)?;
            let mut conn = options.connect().await?;

            let _ = conn
                .execute(&*format!(
                    "CREATE SCHEMA \"{}\"",
                    database.replace('"', "\"\"")
                ))
                .await?;

            Ok(())
        })
    }

    fn database_exists(url: &str) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            let (options, database) = parse_for_maintenance(url)?;
            let mut conn = options.connect().await?;

            let exists: bool = query_scalar("SELECT true FROM exa_schemas WHERE schema_name = ?")
                .bind(database)
                .fetch_one(&mut conn)
                .await?;

            Ok(exists)
        })
    }

    fn drop_database(url: &str) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let (options, database) = parse_for_maintenance(url)?;
            let mut conn = options.connect().await?;

            let _ = conn
                .execute(&*format!("DROP SCHEMA IF EXISTS `{}`", database))
                .await?;

            Ok(())
        })
    }
}

impl Migrate for ExaConnection {
    fn ensure_migrations_table(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            self.execute(
                r#"
CREATE TABLE IF NOT EXISTS _sqlx_migrations (
    version BIGINT,
    description CLOB NOT NULL,
    installed_on TIMESTAMPT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    checksum CLOB NOT NULL,
    execution_time BIGINT NOT NULL
);
                "#,
            )
            .await?;

            Ok(())
        })
    }

    fn dirty_version(&mut self) -> BoxFuture<'_, Result<Option<i64>, MigrateError>> {
        Box::pin(async move {
            let row: Option<(i64,)> = query_as(
                "SELECT version FROM _sqlx_migrations WHERE success = false ORDER BY version LIMIT 1",
            )
            .fetch_optional(self)
            .await?;

            Ok(row.map(|r| r.0))
        })
    }

    fn list_applied_migrations(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async move {
            // language=SQL
            let rows: Vec<(i64, String)> =
                query_as("SELECT version, checksum FROM _sqlx_migrations ORDER BY version")
                    .fetch_all(self)
                    .await?;

            let mut migrations = Vec::with_capacity(rows.len());

            for (version, checksum) in rows {
                let checksum = hex::decode(checksum)
                    .map_err(From::from)
                    .map_err(MigrateError::Source)?
                    .into();

                let migration = AppliedMigration { version, checksum };
                migrations.push(migration);
            }

            Ok(migrations)
        })
    }

    fn lock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move { Ok(()) })
    }

    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move { Ok(()) })
    }

    fn apply<'e: 'm, 'm>(
        &'e mut self,
        migration: &'m Migration,
    ) -> BoxFuture<'m, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let mut tx = self.begin().await?;
            let start = Instant::now();

            // Use a single transaction for the actual migration script and the essential bookeeping so we never
            // execute migrations twice. See https://github.com/launchbadge/sqlx/issues/1966.
            // The `execution_time` however can only be measured for the whole transaction. This value _only_ exists for
            // data lineage and debugging reasons, so it is not super important if it is lost. So we initialize it to -1
            // and update it once the actual transaction completed.
            let _ = tx.execute(&*migration.sql).await?;
            let checksum = hex::encode(&*migration.checksum);

            // language=SQL
            let _ = query(
                r#"
    INSERT INTO _sqlx_migrations ( version, description, success, checksum, execution_time )
    VALUES ( ?, ?, TRUE, ?, -1 )
                "#,
            )
            .bind(migration.version)
            .bind(&*migration.description)
            .bind(checksum)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;

            // Update `elapsed_time`.
            // NOTE: The process may disconnect/die at this point, so the elapsed time value might be lost. We accept
            //       this small risk since this value is not super important.

            let elapsed = start.elapsed();

            let _ = query(r#"UPDATE _sqlx_migrations SET execution_time = ? WHERE version = ?"#)
                .bind(elapsed.as_nanos() as i64)
                .bind(migration.version)
                .execute(self)
                .await?;

            Ok(elapsed)
        })
    }

    fn revert<'e: 'm, 'm>(
        &'e mut self,
        migration: &'m Migration,
    ) -> BoxFuture<'m, Result<Duration, MigrateError>> {
        Box::pin(async move {
            // Use a single transaction for the actual migration script and the essential bookeeping so we never
            // execute migrations twice. See https://github.com/launchbadge/sqlx/issues/1966.
            let mut tx = self.begin().await?;
            let start = Instant::now();

            let _ = tx.execute(&*migration.sql).await?;

            let _ = query(r#"DELETE FROM _sqlx_migrations WHERE version = ?"#)
                .bind(migration.version)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            let elapsed = start.elapsed();

            Ok(elapsed)
        })
    }
}

async fn current_database(conn: &mut ExaConnection) -> Result<String, MigrateError> {
    Ok(query_scalar("SELECT CURRENT_SCHEMA")
        .fetch_one(conn)
        .await?)
}
