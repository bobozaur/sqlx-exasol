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

            let query = format!("CREATE SCHEMA \"{}\"", database.replace('"', "\"\""));
            let _ = conn.execute(&*query).await?;

            Ok(())
        })
    }

    fn database_exists(url: &str) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            let (options, database) = parse_for_maintenance(url)?;
            let mut conn = options.connect().await?;

            let query = "SELECT true FROM exa_schemas WHERE schema_name = ?";
            let exists: bool = query_scalar(query)
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

            let query = format!("DROP SCHEMA IF EXISTS `{}`", database);
            let _ = conn.execute(&*query).await?;

            Ok(())
        })
    }
}

impl Migrate for ExaConnection {
    fn ensure_migrations_table(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            let query = r#"
            CREATE TABLE IF NOT EXISTS "_sqlx_migrations" (
                version BIGINT,
                description CLOB NOT NULL,
                installed_on TIMESTAMPT DEFAULT CURRENT_TIMESTAMP,
                success BOOLEAN NOT NULL,
                checksum CLOB NOT NULL,
                execution_time BIGINT NOT NULL
            );"#;

            self.execute(query).await?;

            Ok(())
        })
    }

    fn dirty_version(&mut self) -> BoxFuture<'_, Result<Option<i64>, MigrateError>> {
        Box::pin(async move {
            let query = r#"
            SELECT version 
            FROM "_sqlx_migrations" 
            WHERE success = false 
            ORDER BY version 
            LIMIT 1
            "#;

            let row: Option<(i64,)> = query_as(query).fetch_optional(self).await?;

            Ok(row.map(|r| r.0))
        })
    }

    fn list_applied_migrations(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async move {
            let query = r#"SELECT version, checksum FROM "_sqlx_migrations" ORDER BY version"#;
            let rows: Vec<(i64, String)> = query_as(query).fetch_all(self).await?;

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

    /// We rely on database transactions for the lock.
    /// Essentially, the executed query does absolutely nothing to the data.
    /// However, it does place a lock on the table which will only be released
    /// when the transaction is over.
    fn lock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            self.ws
                .begin()
                .await
                .map_err(From::from)
                .map_err(MigrateError::Source)?;

            let query = r#"DELETE FROM "_sqlx_migrations" WHERE 1 = 2"#;
            self.execute(query).await?;

            Ok(())
        })
    }

    /// Issues a `ROLLBACK`, thus clearing the transaction, thus releasing the lock.
    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move {
            self.ws
                .rollback()
                .await
                .map_err(From::from)
                .map_err(MigrateError::Source)
        })
    }

    fn apply<'e: 'm, 'm>(
        &'e mut self,
        migration: &'m Migration,
    ) -> BoxFuture<'m, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let mut tx = self.begin().await?;
            let start = Instant::now();

            tx.ws
                .execute_batch(&migration.sql)
                .await
                .map_err(From::from)
                .map_err(MigrateError::Source)?;
            let checksum = hex::encode(&*migration.checksum);

            let query_str = r#"
            INSERT INTO "_sqlx_migrations" ( version, description, success, checksum, execution_time )
            VALUES ( ?, ?, TRUE, ?, -1 )"#;

            let _ = query(query_str)
                .bind(migration.version)
                .bind(&*migration.description)
                .bind(checksum)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            let elapsed = start.elapsed();

            let query_str = r#"UPDATE "_sqlx_migrations" SET execution_time = ? WHERE version = ?"#;
            let _ = query(query_str)
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
            let mut tx = self.begin().await?;
            let start = Instant::now();

            tx.ws
                .execute_batch(&migration.sql)
                .await
                .map_err(From::from)
                .map_err(MigrateError::Source)?;

            let query_str = r#"DELETE FROM "_sqlx_migrations" WHERE version = ?"#;
            let _ = query(query_str)
                .bind(migration.version)
                .execute(&mut *tx)
                .await?;

            tx.commit().await?;

            let elapsed = start.elapsed();

            Ok(elapsed)
        })
    }
}
