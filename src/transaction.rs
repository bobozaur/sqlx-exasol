use futures_core::future::BoxFuture;
use sqlx_core::{transaction::TransactionManager, Error as SqlxError};

use crate::{database::Exasol, ExaConnection};

pub struct ExaTransactionManager;

impl TransactionManager for ExaTransactionManager {
    type Database = Exasol;

    fn begin(conn: &mut ExaConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move { conn.ws.begin().await })
    }

    fn commit(conn: &mut ExaConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move { conn.ws.commit().await })
    }

    fn rollback(conn: &mut ExaConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move { conn.ws.rollback().await })
    }

    fn start_rollback(conn: &mut ExaConnection) {
        conn.ws.pending_rollback = true;
    }
}
