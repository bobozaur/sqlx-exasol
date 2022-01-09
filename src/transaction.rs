use sqlx::{Database, TransactionManager};

use crate::database::Exasol;

pub struct ExaTransactionManager;

impl TransactionManager for ExaTransactionManager {
    type Database = Exasol;

    fn begin(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> futures_util::future::BoxFuture<'_, Result<(), sqlx::Error>> {
        todo!()
    }

    fn commit(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> futures_util::future::BoxFuture<'_, Result<(), sqlx::Error>> {
        todo!()
    }

    fn rollback(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> futures_util::future::BoxFuture<'_, Result<(), sqlx::Error>> {
        todo!()
    }

    fn start_rollback(conn: &mut <Self::Database as Database>::Connection) {
        todo!()
    }
}
