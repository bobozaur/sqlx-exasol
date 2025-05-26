use std::borrow::Cow;

use futures_core::future::BoxFuture;
use futures_util::SinkExt;
use sqlx_core::{transaction::TransactionManager, Error as SqlxError};

use crate::{command::ExaCommand, database::Exasol, ExaConnection};

#[derive(Debug, Clone, Copy)]
pub struct ExaTransactionManager;

impl TransactionManager for ExaTransactionManager {
    type Database = Exasol;

    fn begin<'conn>(
        conn: &'conn mut ExaConnection,
        _: Option<Cow<'static, str>>,
    ) -> BoxFuture<'conn, Result<(), SqlxError>> {
        Box::pin(async move { conn.ws.begin() })
    }

    fn commit(conn: &mut ExaConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move { conn.ws.commit().await })
    }

    fn rollback(conn: &mut ExaConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move { conn.ws.rollback().await })
    }

    fn start_rollback(conn: &mut ExaConnection) {
        // We only need to rollback if the transaction is still open.
        if conn.ws.attributes.open_transaction() {
            conn.ws.attributes.set_autocommit(true);
            conn.ws.attributes.set_open_transaction(false);

            let cmd = ExaCommand::new_execute("ROLLBACK;", conn.attributes())
                .try_into()
                .expect("rollback command should never fail");
            conn.ws.start_send_unpin(cmd).ok();
        }
    }

    fn get_transaction_depth(conn: &ExaConnection) -> usize {
        conn.attributes().open_transaction().into()
    }
}
