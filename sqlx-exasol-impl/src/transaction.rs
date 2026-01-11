use sqlx_core::{sql_str::SqlStr, transaction::TransactionManager};

use crate::{
    connection::websocket::future::{Commit, Rollback, WebSocketFuture},
    database::Exasol,
    error::ExaProtocolError,
    ExaConnection, SqlxResult,
};

/// Implementor of [`TransactionManager`].
#[derive(Debug, Clone, Copy)]
pub struct ExaTransactionManager;

impl TransactionManager for ExaTransactionManager {
    type Database = Exasol;

    async fn begin(conn: &mut ExaConnection, _: Option<SqlStr>) -> SqlxResult<()> {
        // Exasol does not have nested transactions.
        if conn.attributes().open_transaction() {
            // A pending rollback indicates that a transaction was dropped before an explicit
            // rollback, which is why it's still open. If that's the case, then awaiting the
            // rollback is sufficient to proceed.
            match conn.ws.pending_rollback.take() {
                Some(rollback) => rollback.future(&mut conn.ws).await?,
                None => return Err(ExaProtocolError::TransactionAlreadyOpen)?,
            }
        }

        // The next time a request is sent, the transaction will be started.
        // We could eagerly start it as well, but that implies one more round-trip to the server
        // and back with no benefit.
        conn.attributes_mut().set_autocommit(false);
        Ok(())
    }

    async fn commit(conn: &mut ExaConnection) -> SqlxResult<()> {
        Commit::default().future(&mut conn.ws).await
    }

    async fn rollback(conn: &mut ExaConnection) -> SqlxResult<()> {
        Rollback::default().future(&mut conn.ws).await
    }

    fn start_rollback(conn: &mut ExaConnection) {
        conn.ws.pending_rollback = Some(Rollback::default());
    }

    fn get_transaction_depth(conn: &ExaConnection) -> usize {
        conn.attributes().open_transaction().into()
    }
}
