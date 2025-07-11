use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use futures_core::future::BoxFuture;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    migrate::{AppliedMigration, Migrate, MigrateDatabase, MigrateError, Migration},
    query::query,
    query_as::query_as,
    query_scalar::query_scalar,
};

use crate::{
    connection::{
        websocket::future::{ExecuteBatch, WebSocketFuture},
        ExaConnection,
    },
    database::Exasol,
    options::ExaConnectOptions,
    SqlxError, SqlxResult,
};

const LOCK_WARN: &str = "Exasol does not support database locking!";

fn parse_for_maintenance(url: &str) -> SqlxResult<(ExaConnectOptions, String)> {
    let mut options = ExaConnectOptions::from_str(url)?;

    let database = options.schema.ok_or_else(|| {
        SqlxError::Configuration("DATABASE_URL does not specify a database".into())
    })?;

    // switch to <no> database for create/drop commands
    options.schema = None;

    Ok((options, database))
}

impl MigrateDatabase for Exasol {
    fn create_database(url: &str) -> BoxFuture<'_, SqlxResult<()>> {
        Box::pin(async move {
            let (options, database) = parse_for_maintenance(url)?;
            let mut conn = options.connect().await?;

            let query = format!("CREATE SCHEMA \"{}\"", database.replace('"', "\"\""));
            let _ = conn.execute(&*query).await?;

            Ok(())
        })
    }

    fn database_exists(url: &str) -> BoxFuture<'_, SqlxResult<bool>> {
        Box::pin(async move {
            let (options, database) = parse_for_maintenance(url)?;
            let mut conn = options.connect().await?;

            let query = "SELECT true FROM exa_schemas WHERE schema_name = ?";
            let exists: bool = query_scalar(query)
                .bind(database)
                .fetch_optional(&mut conn)
                .await?
                .unwrap_or_default();

            Ok(exists)
        })
    }

    fn drop_database(url: &str) -> BoxFuture<'_, SqlxResult<()>> {
        Box::pin(async move {
            let (options, database) = parse_for_maintenance(url)?;
            let mut conn = options.connect().await?;

            let query = format!(r#"DROP SCHEMA IF EXISTS "{database}""#);
            let _ = conn.execute(&*query).await?;

            Ok(())
        })
    }
}

impl Migrate for ExaConnection {
    fn create_schema_if_not_exists<'e>(
        &'e mut self,
        schema_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            let query = format!(r#"CREATE SCHEMA IF NOT EXISTS "{schema_name}";"#);
            self.execute(&*query).await?;
            Ok(())
        })
    }

    fn ensure_migrations_table<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            let query = format!(
                r#"
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                version DECIMAL(20, 0),
                description CLOB NOT NULL,
                installed_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                success BOOLEAN NOT NULL,
                checksum CLOB NOT NULL,
                execution_time DECIMAL(20, 0) NOT NULL
            );"#
            );

            self.execute(&*query).await?;
            Ok(())
        })
    }

    fn dirty_version<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<Option<i64>, MigrateError>> {
        Box::pin(async move {
            let query = format!(
                r#"
            SELECT version
            FROM "{table_name}"
            WHERE success = false
            ORDER BY version
            LIMIT 1;
            "#
            );

            let row: Option<(i64,)> = query_as(&query).fetch_optional(self).await?;

            Ok(row.map(|r| r.0))
        })
    }

    fn list_applied_migrations<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async move {
            let query = format!(
                r#"
                SELECT version, checksum
                FROM "{table_name}"
                ORDER BY version
                "#
            );

            let rows: Vec<(i64, String)> = query_as(&query).fetch_all(self).await?;
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
        Box::pin(async move {
            tracing::warn!("{LOCK_WARN}");
            Ok(())
        })
    }

    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            tracing::warn!("{LOCK_WARN}");
            Ok(())
        })
    }

    fn apply<'e>(
        &'e mut self,
        table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let mut tx = self.begin().await?;
            let start = Instant::now();

            ExecuteBatch::new(migration.sql.as_ref())
                .future(&mut tx.ws)
                .await?;

            let checksum = hex::encode(&*migration.checksum);

            let query_str = format!(
                r#"
            INSERT INTO "{table_name}" ( version, description, success, checksum, execution_time )
            VALUES ( ?, ?, TRUE, ?, -1 );
            "#
            );

            let _ = query(&query_str)
                .bind(migration.version)
                .bind(&*migration.description)
                .bind(checksum)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            let elapsed = start.elapsed();

            let query_str = format!(
                r#"
                UPDATE "{table_name}"
                SET execution_time = ?
                WHERE version = ?
                "#
            );

            let _ = query(&query_str)
                .bind(elapsed.as_nanos())
                .bind(migration.version)
                .execute(self)
                .await?;

            Ok(elapsed)
        })
    }

    fn revert<'e>(
        &'e mut self,
        table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let mut tx = self.begin().await?;
            let start = Instant::now();

            ExecuteBatch::new(migration.sql.as_ref())
                .future(&mut tx.ws)
                .await?;

            let query_str = format!(r#" DELETE FROM "{table_name}" WHERE version = ? "#);
            let _ = query(&query_str)
                .bind(migration.version)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            let elapsed = start.elapsed();

            Ok(elapsed)
        })
    }
}
