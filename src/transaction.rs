use std::borrow::Cow;

use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use sqlx_core::{transaction::TransactionManager, Error as SqlxError};

use crate::{
    connection::futures::{Commit, Rollback, SetAttributes, WebSocketFuture},
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
            let attributes = conn.attributes_mut();
            // Exasol does not have nested transactions.
            if attributes.open_transaction() {
                return Err(ExaProtocolError::TransactionAlreadyOpen)?;
            }

            attributes.set_autocommit(false);
            SetAttributes::default().future(&mut conn.ws).await
        })
    }

    fn commit(conn: &mut ExaConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        async move { Commit::default().future(&mut conn.ws).await }.boxed()
    }

    fn rollback(conn: &mut ExaConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        async move { Rollback::default().future(&mut conn.ws).await }.boxed()
    }

    fn start_rollback(conn: &mut ExaConnection) {
        let attributes = conn.attributes_mut();

        // We only need to rollback if a transaction is open.
        if attributes.open_transaction() {
            attributes.set_autocommit(true);
        }
    }

    fn get_transaction_depth(conn: &ExaConnection) -> usize {
        conn.attributes().open_transaction().into()
    }
}
