#[cfg(feature = "etl")]
pub mod etl;
mod executor;
pub mod stream;
pub mod websocket;

use std::net::SocketAddr;

use futures_core::future::BoxFuture;
use futures_util::{FutureExt, SinkExt};
use rand::{seq::SliceRandom, thread_rng};
use sqlx_core::{
    connection::{Connection, LogSettings},
    executor::Executor,
    transaction::Transaction,
};
use websocket::{socket::WithExaSocket, ExaWebSocket};

use crate::{
    connection::websocket::{
        future::{ClosePrepared, Disconnect, SetAttributes, WebSocketFuture},
        WithMaybeTlsExaSocket,
    },
    database::Exasol,
    options::ExaConnectOptions,
    responses::{ExaAttributes, SessionInfo},
    SqlxError, SqlxResult,
};

/// A connection to the Exasol database. Implementor of [`Connection`].
#[derive(Debug)]
pub struct ExaConnection {
    pub(crate) ws: ExaWebSocket,
    pub(crate) log_settings: LogSettings,
    session_info: SessionInfo,
}

impl ExaConnection {
    /// Returns the Exasol server socket address that we're connected to.
    pub fn server(&self) -> SocketAddr {
        self.ws.server()
    }

    /// Returns a reference of the [`ExaAttributes`] used in this connection.
    pub fn attributes(&self) -> &ExaAttributes {
        &self.ws.attributes
    }

    /// Allows setting connection attributes through the driver.
    ///
    /// Note that attributes will only reach Exasol after a statement is
    /// executed or after explicitly calling the `flush_attributes()` method.
    pub fn attributes_mut(&mut self) -> &mut ExaAttributes {
        &mut self.ws.attributes
    }

    /// Flushes the current [`ExaAttributes`] to Exasol.
    ///
    /// # Errors
    ///
    /// Will return an error if sending the attributes fails.
    pub async fn flush_attributes(&mut self) -> SqlxResult<()> {
        SetAttributes::default().future(&mut self.ws).await
    }

    /// Returns a reference to the [`SessionInfo`] related to this connection.
    pub fn session_info(&self) -> &SessionInfo {
        &self.session_info
    }

    pub(crate) async fn establish(opts: &ExaConnectOptions) -> SqlxResult<Self> {
        let mut ws_result = Err(SqlxError::Configuration("No hosts found".into()));

        // We want to try and randomly connect to nodes, if multiple were provided.
        // But since the RNG is not Send and cloning the hosts would be more expensive,
        // we create a vector of the indices and shuffle these instead.
        let mut indices = (0..opts.hosts_details.len()).collect::<Vec<_>>();
        indices.shuffle(&mut thread_rng());

        // For each host parsed from the connection string
        for idx in indices {
            // We know the index is valid since it's between 0..hosts.len()
            let (host, addrs) = opts
                .hosts_details
                .get(idx)
                .expect("hosts list index must be valid");

            // For each socket address resolved from the host
            for socket_addr in addrs {
                let wrapper = WithExaSocket(*socket_addr);
                let with_socket = WithMaybeTlsExaSocket::new(wrapper, host, opts.into());
                let socket_res = sqlx_core::net::connect_tcp(host, opts.port, with_socket).await;

                // Continue if the future to connect a socket failed
                let (socket, with_tls) = match socket_res {
                    Ok(Ok((socket, with_tls))) => (socket, with_tls),
                    Ok(Err(err)) | Err(err) => {
                        ws_result = Err(err);
                        continue;
                    }
                };

                // Break if we successfully connect a websocket.
                match ExaWebSocket::new(host, opts.port, socket, opts.try_into()?, with_tls).await {
                    Ok(ws) => {
                        ws_result = Ok(ws);
                        break;
                    }
                    Err(err) => ws_result = Err(err),
                }
            }
        }

        let (ws, session_info) = ws_result?;
        let mut con = Self {
            ws,
            log_settings: LogSettings::default(),
            session_info,
        };

        con.configure_session().await?;

        Ok(con)
    }

    /// Sets session parameters for the open connection.
    async fn configure_session(&mut self) -> SqlxResult<()> {
        // We rely on this for consistent size output for HASHTYPE columns.
        // This allows to reliably use UUID at compile-time.
        self.execute("ALTER SESSION SET HASHTYPE_FORMAT = 'HEX';")
            .await?;
        Ok(())
    }
}

impl Connection for ExaConnection {
    type Database = Exasol;

    type Options = ExaConnectOptions;

    fn close(mut self) -> BoxFuture<'static, SqlxResult<()>> {
        Box::pin(async move {
            Disconnect::default().future(&mut self.ws).await?;
            self.ws.close().await?;
            Ok(())
        })
    }

    fn close_hard(mut self) -> BoxFuture<'static, SqlxResult<()>> {
        Box::pin(async move { self.ws.close().await })
    }

    fn ping(&mut self) -> BoxFuture<'_, SqlxResult<()>> {
        self.ws.ping().boxed()
    }

    fn begin(&mut self) -> BoxFuture<'_, SqlxResult<Transaction<'_, Self::Database>>>
    where
        Self: Sized,
    {
        Transaction::begin(self, None)
    }

    fn shrink_buffers(&mut self) {}

    fn flush(&mut self) -> BoxFuture<'_, SqlxResult<()>> {
        Box::pin(async {
            if let Some(future) = self.ws.pending_close.take() {
                future.future(&mut self.ws).await?;
            }

            if let Some(future) = self.ws.pending_rollback.take() {
                future.future(&mut self.ws).await?;
            }

            Ok(())
        })
    }

    fn should_flush(&self) -> bool {
        self.ws.pending_close.is_some() || self.ws.pending_rollback.is_some()
    }

    fn cached_statements_size(&self) -> usize
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        self.ws.statement_cache.len()
    }

    fn clear_cached_statements(&mut self) -> BoxFuture<'_, SqlxResult<()>>
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        Box::pin(async {
            while let Some((_, prep)) = self.ws.statement_cache.pop_lru() {
                ClosePrepared::new(prep.statement_handle)
                    .future(&mut self.ws)
                    .await?;
            }

            Ok(())
        })
    }
}

#[cfg(test)]
#[cfg(feature = "migrate")]
#[allow(clippy::large_futures, reason = "silencing clippy")]
mod tests {
    use std::num::NonZeroUsize;

    use futures_util::TryStreamExt;
    use sqlx::Executor;
    use sqlx_core::{error::BoxDynError, pool::PoolOptions};

    use crate::{ExaConnectOptions, Exasol};

    #[sqlx::test]
    async fn test_stmt_cache(
        pool_opts: PoolOptions<Exasol>,
        mut exa_opts: ExaConnectOptions,
    ) -> Result<(), BoxDynError> {
        // Set a low cache size
        exa_opts.statement_cache_capacity = NonZeroUsize::new(1).unwrap();

        let pool = pool_opts.connect_with(exa_opts).await?;
        let mut con = pool.acquire().await?;

        let sql1 = "SELECT 1 FROM dual";
        let sql2 = "SELECT 2 FROM dual";

        assert!(!con.as_ref().ws.statement_cache.contains(sql1));
        assert!(!con.as_ref().ws.statement_cache.contains(sql2));

        sqlx::query(sql1).execute(&mut *con).await?;
        assert!(con.as_ref().ws.statement_cache.contains(sql1));
        assert!(!con.as_ref().ws.statement_cache.contains(sql2));

        sqlx::query(sql2).execute(&mut *con).await?;
        assert!(!con.as_ref().ws.statement_cache.contains(sql1));
        assert!(con.as_ref().ws.statement_cache.contains(sql2));

        Ok(())
    }

    #[sqlx::test]
    async fn test_schema_none_selected(
        pool_opts: PoolOptions<Exasol>,
        mut exa_opts: ExaConnectOptions,
    ) -> Result<(), BoxDynError> {
        exa_opts.schema = None;

        let pool = pool_opts.connect_with(exa_opts).await?;
        let mut con = pool.acquire().await?;

        let schema: Option<String> = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
            .fetch_one(&mut *con)
            .await?;

        assert!(schema.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn test_connection_result_set_auto_close(
        pool_opts: PoolOptions<Exasol>,
        exa_opts: ExaConnectOptions,
    ) -> Result<(), BoxDynError> {
        // Only allow one connection
        let pool = pool_opts.max_connections(1).connect_with(exa_opts).await?;
        let mut conn = pool.acquire().await?;
        conn.execute("CREATE TABLE CLOSE_RESULTS_TEST ( col DECIMAL(3, 0) );")
            .await?;

        sqlx::query("INSERT INTO CLOSE_RESULTS_TEST VALUES(?)")
            .bind(vec![1i8; 10000])
            .execute(&mut *conn)
            .await?;

        assert!(conn.ws.pending_close.is_none());
        let _ = conn
            .fetch("SELECT * FROM CLOSE_RESULTS_TEST")
            .try_next()
            .await?;

        assert!(conn.ws.pending_close.is_some());
        let _ = conn
            .fetch("SELECT * FROM CLOSE_RESULTS_TEST")
            .try_next()
            .await;

        assert!(conn.ws.pending_close.is_some());
        let _ = conn
            .fetch("SELECT * FROM CLOSE_RESULTS_TEST")
            .try_next()
            .await;

        assert!(conn.ws.pending_close.is_some());
        let _ = conn
            .fetch("SELECT * FROM CLOSE_RESULTS_TEST")
            .try_next()
            .await;

        assert!(conn.ws.pending_close.is_some());
        conn.flush_attributes().await?;

        assert!(conn.ws.pending_close.is_none());
        Ok(())
    }
}
