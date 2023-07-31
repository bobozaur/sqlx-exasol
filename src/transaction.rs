use sqlx_core::{database::Database, transaction::TransactionManager, Error as SqlxError};

use crate::database::Exasol;

pub struct ExaTransactionManager;

impl TransactionManager for ExaTransactionManager {
    type Database = Exasol;

    fn begin(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> futures_util::future::BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move { conn.ws.begin().await })
    }

    fn commit(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> futures_util::future::BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move { conn.ws.commit().await })
    }

    fn rollback(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> futures_util::future::BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move { conn.ws.rollback().await })
    }

    fn start_rollback(_conn: &mut <Self::Database as Database>::Connection) {}
}
