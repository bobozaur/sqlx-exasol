use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use futures_core::future::BoxFuture;
use sqlx_core::{
    connection::{ConnectOptions, Connection},
    executor::Executor,
    migrate::{AppliedMigration, Migrate, MigrateDatabase, MigrateError, Migration},
    sql_str::AssertSqlSafe,
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

    let database = options
        .schema
        .ok_or_else(|| SqlxError::Configuration("DATABASE_URL does not specify a database".into()))
        // Escape double quotes because we'll quote the database name in constructed queries.
        .map(|db| db.replace('"', "\"\""))?;

    // switch to <no> database for create/drop commands
    options.schema = None;

    Ok((options, database))
}

impl MigrateDatabase for Exasol {
    async fn create_database(url: &str) -> SqlxResult<()> {
        let (options, database) = parse_for_maintenance(url)?;
        let mut conn = options.connect().await?;

        let query = format!(r#"CREATE SCHEMA "{database}";"#);
        conn.execute(AssertSqlSafe(query)).await?;

        Ok(())
    }

    async fn database_exists(url: &str) -> SqlxResult<bool> {
        let (options, database) = parse_for_maintenance(url)?;
        let mut conn = options.connect().await?;

        let query = "SELECT true FROM exa_schemas WHERE schema_name = ?";
        let exists: bool = sqlx_core::query_scalar::query_scalar(query)
            .bind(database)
            .fetch_optional(&mut conn)
            .await?
            .unwrap_or_default();

        Ok(exists)
    }

    async fn drop_database(url: &str) -> SqlxResult<()> {
        let (options, database) = parse_for_maintenance(url)?;
        let mut conn = options.connect().await?;

        let query = format!(r#"DROP SCHEMA IF EXISTS "{database}" CASCADE;"#);
        conn.execute(AssertSqlSafe(query)).await?;

        Ok(())
    }
}

impl Migrate for ExaConnection {
    fn create_schema_if_not_exists<'e>(
        &'e mut self,
        schema_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            let query = format!(r#"CREATE SCHEMA IF NOT EXISTS "{schema_name}";"#);
            self.execute(AssertSqlSafe(query)).await?;
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
            self.execute(AssertSqlSafe(query)).await?;
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
            let row: Option<(i64,)> = sqlx_core::query_as::query_as(AssertSqlSafe(query))
                .fetch_optional(self)
                .await?;
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

            let rows: Vec<(i64, String)> = sqlx_core::query_as::query_as(AssertSqlSafe(query))
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

            ExecuteBatch::new(migration.sql.clone())
                .future(&mut tx.ws)
                .await?;

            let checksum = hex::encode(&*migration.checksum);

            let query = format!(
                r#"
            INSERT INTO "{table_name}" ( version, description, success, checksum, execution_time )
            VALUES ( ?, ?, TRUE, ?, -1 );
            "#
            );

            let _ = sqlx_core::query::query(AssertSqlSafe(query))
                .bind(migration.version)
                .bind(&*migration.description)
                .bind(checksum)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            let elapsed = start.elapsed();

            let query = format!(
                r#"
                UPDATE "{table_name}"
                SET execution_time = ?
                WHERE version = ?
                "#
            );

            #[allow(clippy::cast_possible_truncation)]
            let _ = sqlx_core::query::query(AssertSqlSafe(query))
                .bind(elapsed.as_nanos() as i64)
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

            ExecuteBatch::new(migration.sql.clone())
                .future(&mut tx.ws)
                .await?;

            let query = format!(r#" DELETE FROM "{table_name}" WHERE version = ? "#);
            let _ = sqlx_core::query::query(AssertSqlSafe(query))
                .bind(migration.version)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            let elapsed = start.elapsed();

            Ok(elapsed)
        })
    }
}
