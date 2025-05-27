#[cfg(feature = "etl")]
pub mod etl;
mod executor;
mod query_splitter;
mod stream;
mod websocket;

use std::{iter, net::SocketAddr};

use futures_util::SinkExt;
use lru::LruCache;
use rand::{seq::SliceRandom, thread_rng};
use sqlx_core::{
    connection::{Connection, LogSettings},
    transaction::Transaction,
    Error as SqlxError,
};
use stream::QueryResultStream;
use websocket::{socket::WithExaSocket, ExaWebSocket};

use self::websocket::WithMaybeTlsExaSocket;
use crate::{
    arguments::ExaArguments,
    command::ExaCommand,
    database::Exasol,
    error::ExaProtocolError,
    options::ExaConnectOptions,
    responses::{ExaAttributes, PreparedStatement, SessionInfo},
};

/// A connection to the Exasol database.
#[derive(Debug)]
pub struct ExaConnection {
    pub(crate) ws: ExaWebSocket,
    session_info: SessionInfo,
    statement_cache: LruCache<String, PreparedStatement>,
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
        self.ws.set_attributes().await
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
            statement_cache: LruCache::new(opts.statement_cache_capacity),
            log_settings: LogSettings::default(),
            session_info,
        };

        Ok(con)
    }

    async fn execute_query<'a>(
        &'a mut self,
        sql: &str,
        arguments: Option<ExaArguments>,
        persist: bool,
    ) -> Result<QueryResultStream<'a>, SqlxError> {
        match arguments {
            Some(args) => self.execute_prepared(sql, args, persist).await,
            None => self.execute_plain(sql).await,
        }
    }

    async fn execute_prepared<'a>(
        &'a mut self,
        sql: &str,
        args: ExaArguments,
        persist: bool,
    ) -> Result<QueryResultStream<'a>, SqlxError> {
        let prepared = self
            .ws
            .get_or_prepare(&mut self.statement_cache, sql, persist)
            .await?;

        // Check the compatibility between provided parameter data types
        // and the ones expected by the database.
        for (expected, provided) in iter::zip(prepared.parameters.as_ref(), &args.types) {
            if !expected.compatible(provided) {
                Err(ExaProtocolError::DatatypeMismatch(*expected, *provided))?;
            }
        }

        let cmd = ExaCommand::new_execute_prepared(
            prepared.statement_handle,
            &prepared.parameters,
            args.buf,
            &self.ws.attributes,
        )
        .try_into()?;

        self.ws
            .get_result_stream(cmd, &mut self.last_rs_handle)
            .await
    }

    async fn execute_plain<'a>(
        &'a mut self,
        sql: &str,
    ) -> Result<QueryResultStream<'a>, SqlxError> {
        let cmd = ExaCommand::new_execute(sql, &self.ws.attributes).try_into()?;
        self.ws
            .get_result_stream(cmd, &mut self.last_rs_handle)
            .await
    }
}

impl Connection for ExaConnection {
    type Database = Exasol;

    type Options = ExaConnectOptions;

    fn close(mut self) -> futures_util::future::BoxFuture<'static, Result<(), SqlxError>> {
        Box::pin(async move {
            self.ws.disconnect().await?;
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
        Box::pin(async move {
            self.ws.ping().await?;
            Ok(())
        })
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
        self.statement_cache.len()
    }

    fn clear_cached_statements(
        &mut self,
    ) -> futures_core::future::BoxFuture<'_, Result<(), SqlxError>>
    where
        Self::Database: sqlx_core::database::HasStatementCache,
    {
        Box::pin(async {
            while let Some((_, prep)) = self.statement_cache.pop_lru() {
                self.ws.close_prepared(prep.statement_handle).await?;
            }

            Ok(())
        })
    }
}

#[cfg(test)]
#[cfg(feature = "migrate")]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use sqlx::{query, Connection, Executor};
    use sqlx_core::{error::BoxDynError, pool::PoolOptions};

    use crate::{ExaConnectOptions, Exasol};

    #[cfg(feature = "compression")]
    #[ignore]
    #[sqlx::test]
    async fn test_compression_works(
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
    async fn test_compression_no_flag(
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

        assert!(!con.as_ref().statement_cache.contains(sql1));
        assert!(!con.as_ref().statement_cache.contains(sql2));

        query(sql1).execute(&mut *con).await?;
        assert!(con.as_ref().statement_cache.contains(sql1));
        assert!(!con.as_ref().statement_cache.contains(sql2));

        query(sql2).execute(&mut *con).await?;
        assert!(!con.as_ref().statement_cache.contains(sql1));
        assert!(con.as_ref().statement_cache.contains(sql2));

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
        mut exa_opts: ExaConnectOptions,
    ) -> Result<(), BoxDynError> {
        // Set a low cache size
        exa_opts.statement_cache_capacity = NonZeroUsize::new(1).unwrap();

        let pool = pool_opts.connect_with(exa_opts).await?;

        {
            let mut conn = pool.acquire().await?;

            {
                let mut tx = conn.begin().await?;
                tx.execute("select * from EXA_TIME_ZONES limit 1800")
                    .await?;
            }

            // assert!(conn.ws.pending_rollback);
            todo!();
        }

        // Should be plenty of time to execute the flush
        // after the connection is dropped.
        tokio::time::sleep(Duration::from_secs(5)).await;
        // assert!(!pool.acquire().await?.ws.pending_rollback);
        todo!();

        Ok(())
    }
}
