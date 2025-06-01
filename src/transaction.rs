use std::borrow::Cow;

use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use sqlx_core::{transaction::TransactionManager, Error as SqlxError};

use crate::{
    connection::websocket::future::{Commit, Rollback, WebSocketFuture},
    database::Exasol,
    error::ExaProtocolError,
    ExaConnection,
};

#[derive(Debug, Clone, Copy)]
pub struct ExaTransactionManager;

impl TransactionManager for ExaTransactionManager {
    type Database = Exasol;

    fn begin<'conn>(
        conn: &'conn mut ExaConnection,
        _: Option<Cow<'static, str>>,
    ) -> BoxFuture<'conn, Result<(), SqlxError>> {
        Box::pin(async {
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
            // We could eagerly start it as well, but that implies one more
            // round-trip to the server and back with no benefit.
            #[expect(deprecated, reason = "will make this private")]
            conn.attributes_mut().set_autocommit(false);
            Ok(())
        })
    }

    fn commit(conn: &mut ExaConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        async move { Commit::default().future(&mut conn.ws).await }.boxed()
    }

    fn rollback(conn: &mut ExaConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        async move { Rollback::default().future(&mut conn.ws).await }.boxed()
    }

    fn start_rollback(conn: &mut ExaConnection) {
        conn.ws.pending_rollback = Some(Rollback::default());
    }

    fn get_transaction_depth(conn: &ExaConnection) -> usize {
        conn.attributes().open_transaction().into()
    }
}
