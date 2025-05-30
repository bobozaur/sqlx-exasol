#[cfg(feature = "etl")]
pub mod etl;
mod executor;
mod query_splitter;
mod stream;
mod websocket;

use std::net::SocketAddr;

use futures_util::SinkExt;
use rand::{seq::SliceRandom, thread_rng};
use sqlx_core::{
    connection::{Connection, LogSettings},
    transaction::Transaction,
    Error as SqlxError,
};
pub use websocket::futures;
use websocket::{socket::WithExaSocket, ExaWebSocket};

use self::websocket::WithMaybeTlsExaSocket;
use crate::{
    connection::futures::{ClosePrepared, Disconnect, SetAttributes, WebSocketFuture},
    database::Exasol,
    options::ExaConnectOptions,
    responses::{ExaAttributes, SessionInfo},
};

/// A connection to the Exasol database.
#[derive(Debug)]
pub struct ExaConnection {
    pub(crate) ws: ExaWebSocket,
    session_info: SessionInfo,
    log_settings: LogSettings,
}

impl ExaConnection {
    /// Returns the socket address that we're connected to.
    pub fn socket_addr(&self) -> SocketAddr {
        self.ws.socket_addr()
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
    pub async fn flush_attributes(&mut self) -> Result<(), SqlxError> {
        SetAttributes::default().future(&mut self.ws).await
    }

    /// Returns a reference to the [`SessionInfo`] related to this connection.
    pub fn session_info(&self) -> &SessionInfo {
        &self.session_info
    }

    pub(crate) async fn establish(opts: &ExaConnectOptions) -> Result<Self, SqlxError> {
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
                match ExaWebSocket::new(host, opts.port, socket, opts.into(), with_tls).await {
                    Ok(ws) => {
                        ws_result = Ok(ws);
                        break;
                    }
                    Err(err) => ws_result = Err(err),
                }
            }
        }

        let (ws, session_info) = ws_result?;
        let con = Self {
            ws,
            log_settings: LogSettings::default(),
            session_info,
        };

        Ok(con)
    }
}

impl Connection for ExaConnection {
    type Database = Exasol;

    type Options = ExaConnectOptions;

    fn close(mut self) -> futures_util::future::BoxFuture<'static, Result<(), SqlxError>> {
        Box::pin(async move {
            Disconnect::default().future(&mut self.ws).await?;
            self.ws.close().await?;
            Ok(())
        })
    }

    fn close_hard(mut self) -> futures_util::future::BoxFuture<'static, Result<(), SqlxError>> {
        Box::pin(async move {
            self.ws.close().await?;
            Ok(())
        })
    }

    fn ping(&mut self) -> futures_util::future::BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(self.ws.ping())
    }

    fn begin(
        &mut self,
    ) -> futures_util::future::BoxFuture<'_, Result<Transaction<'_, Self::Database>, SqlxError>>
    where
        Self: Sized,
    {
        Transaction::begin(self, None)
    }

    fn shrink_buffers(&mut self) {}

    fn flush(&mut self) -> futures_util::future::BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn should_flush(&self) -> bool {
        false
    }

    fn cached_statements_size(&self) -> usize
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        self.ws.statement_cache.len()
    }

    fn clear_cached_statements(
        &mut self,
    ) -> futures_core::future::BoxFuture<'_, Result<(), SqlxError>>
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
mod tests {
    use std::num::NonZeroUsize;

    use futures_util::TryStreamExt;
    use sqlx::{query, Connection, Executor};
    use sqlx_core::{error::BoxDynError, pool::PoolOptions};

    use crate::{ExaConnectOptions, Exasol};

    #[cfg(feature = "compression")]
    #[ignore]
    #[sqlx::test]
    async fn test_compression_feature(
        pool_opts: PoolOptions<Exasol>,
        mut exa_opts: ExaConnectOptions,
    ) -> Result<(), BoxDynError> {
        exa_opts.compression = true;

        let pool = pool_opts.connect_with(exa_opts).await?;
        let mut con = pool.acquire().await?;
        let schema = "TEST_SWITCH_SCHEMA";

        con.execute(format!("CREATE SCHEMA IF NOT EXISTS {schema};").as_str())
            .await?;

        let new_schema: String = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
            .fetch_one(&mut *con)
            .await?;

        con.execute(format!("DROP SCHEMA IF EXISTS {schema} CASCADE;").as_str())
            .await?;

        assert_eq!(schema, new_schema);

        Ok(())
    }

    #[cfg(not(feature = "compression"))]
    #[sqlx::test]
    async fn test_compression_no_feature(
        pool_opts: PoolOptions<Exasol>,
        mut exa_opts: ExaConnectOptions,
    ) {
        exa_opts.compression = true;
        assert!(pool_opts.connect_with(exa_opts).await.is_err());
    }

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

        query(sql1).execute(&mut *con).await?;
        assert!(con.as_ref().ws.statement_cache.contains(sql1));
        assert!(!con.as_ref().ws.statement_cache.contains(sql2));

        query(sql2).execute(&mut *con).await?;
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
    async fn test_schema_selected(
        pool_opts: PoolOptions<Exasol>,
        exa_opts: ExaConnectOptions,
    ) -> Result<(), BoxDynError> {
        let pool = pool_opts.connect_with(exa_opts).await?;
        let mut con = pool.acquire().await?;

        let schema: Option<String> = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
            .fetch_one(&mut *con)
            .await?;

        assert!(schema.is_some());

        Ok(())
    }

    #[sqlx::test]
    async fn test_schema_switch(
        pool_opts: PoolOptions<Exasol>,
        exa_opts: ExaConnectOptions,
    ) -> Result<(), BoxDynError> {
        let pool = pool_opts.connect_with(exa_opts).await?;
        let mut con = pool.acquire().await?;
        let schema = "TEST_SWITCH_SCHEMA";

        con.execute(format!("CREATE SCHEMA IF NOT EXISTS {schema};").as_str())
            .await?;

        let new_schema: String = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
            .fetch_one(&mut *con)
            .await?;

        con.execute(format!("DROP SCHEMA IF EXISTS {schema} CASCADE;").as_str())
            .await?;

        assert_eq!(schema, new_schema);

        Ok(())
    }

    #[sqlx::test]
    async fn test_schema_switch_from_attr(
        pool_opts: PoolOptions<Exasol>,
        exa_opts: ExaConnectOptions,
    ) -> Result<(), BoxDynError> {
        let pool = pool_opts.connect_with(exa_opts).await?;
        let mut con = pool.acquire().await?;

        let orig_schema: String = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
            .fetch_one(&mut *con)
            .await?;

        let schema = "TEST_SWITCH_SCHEMA";

        con.execute(format!("CREATE SCHEMA IF NOT EXISTS {schema};").as_str())
            .await?;

        con.attributes_mut().set_current_schema(orig_schema.clone());
        con.flush_attributes().await?;

        let new_schema: String = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
            .fetch_one(&mut *con)
            .await?;

        assert_eq!(orig_schema, new_schema);

        Ok(())
    }

    #[sqlx::test]
    async fn test_schema_close_and_empty_attr(
        pool_opts: PoolOptions<Exasol>,
        exa_opts: ExaConnectOptions,
    ) -> Result<(), BoxDynError> {
        let pool = pool_opts.connect_with(exa_opts).await?;
        let mut con = pool.acquire().await?;

        let orig_schema: String = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
            .fetch_one(&mut *con)
            .await?;

        assert_eq!(
            con.attributes().current_schema(),
            Some(orig_schema.as_str())
        );

        con.execute("CLOSE SCHEMA").await?;
        assert_eq!(con.attributes().current_schema(), None);

        Ok(())
    }

    #[sqlx::test]
    async fn test_connection_flush_on_drop(
        pool_opts: PoolOptions<Exasol>,
        exa_opts: ExaConnectOptions,
    ) -> Result<(), BoxDynError> {
        // Only allow one connection
        let pool = pool_opts.max_connections(1).connect_with(exa_opts).await?;
        pool.execute("CREATE TABLE TRANSACTIONS_TEST ( col DECIMAL(1, 0) );")
            .await?;

        {
            let mut conn = pool.acquire().await?;
            let mut tx = conn.begin().await?;
            tx.execute("INSERT INTO TRANSACTIONS_TEST VALUES(1)")
                .await?;
        }

        let mut conn = pool.acquire().await?;
        {
            let mut tx = conn.begin().await?;
            tx.execute("INSERT INTO TRANSACTIONS_TEST VALUES(1)")
                .await?;
        }

        {
            let mut tx = conn.begin().await?;
            tx.execute("INSERT INTO TRANSACTIONS_TEST VALUES(1)")
                .await?;
        }

        drop(conn);

        let inserted = pool
            .fetch_all("SELECT * FROM TRANSACTIONS_TEST")
            .await?
            .len();

        assert_eq!(inserted, 0);
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
        conn.execute("CREATE TABLE CLOSE_RESULTS_TEST ( col DECIMAL(1, 0) );")
            .await?;

        query("INSERT INTO CLOSE_RESULTS_TEST VALUES(?)")
            .bind([1; 10000])
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
