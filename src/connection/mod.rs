mod executor;
mod http_transport;
mod macros;
mod stream;
mod tls;
mod websocket;

use std::{iter, net::SocketAddr};

use lru::LruCache;
use sqlx_core::{
    connection::{Connection, LogSettings},
    transaction::Transaction,
    Error as SqlxError,
};

use futures_util::{Future, TryStreamExt};

use crate::{
    arguments::ExaArguments,
    command::ExaCommand,
    database::Exasol,
    error::ExaProtocolError,
    options::ExaConnectOptions,
    responses::{DataChunk, ExaAttributes, PreparedStatement, SessionInfo},
    ExaQueryResult,
};

use stream::QueryResultStream;
use websocket::{socket::WithExaSocket, ExaWebSocket};

use self::macros::fetcher_closure;

pub use http_transport::{
    ExaExport, ExaImport, ExportOptions, ImportOptions, QueryOrTable, RowSeparator, Trim,
};

#[derive(Debug)]
pub struct ExaConnection {
    pub(crate) ws: ExaWebSocket,
    pub(crate) last_rs_handle: Option<u16>,
    session_info: SessionInfo,
    statement_cache: LruCache<String, PreparedStatement>,
    log_settings: LogSettings,
}

impl ExaConnection {
    pub fn socket_addr(&self) -> SocketAddr {
        self.ws.socket_addr()
    }

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
    pub async fn flush_attributes(&mut self) -> Result<(), SqlxError> {
        self.ws.set_attributes().await
    }

    pub fn session_info(&self) -> &SessionInfo {
        &self.session_info
    }

    pub(crate) async fn establish(opts: &ExaConnectOptions) -> Result<Self, SqlxError> {
        let mut ws_result = Err(SqlxError::Configuration("No hosts found".into()));

        for host in &opts.hosts {
            let str_host = host.to_string();
            let socket_addr = SocketAddr::new(*host, opts.port);
            let with_socket = WithExaSocket(socket_addr);
            let socket_res = sqlx_core::net::connect_tcp(&str_host, opts.port, with_socket).await;

            let socket = match socket_res {
                Ok(socket) => socket,
                Err(err) => {
                    ws_result = Err(err);
                    continue;
                }
            };

            match ExaWebSocket::new(&str_host, socket, opts.into()).await {
                Ok(ws) => {
                    ws_result = Ok(ws);
                    break;
                }
                Err(err) => {
                    ws_result = Err(err);
                    continue;
                }
            }
        }
        let (ws, session_info) = ws_result?;
        let con = Self {
            ws,
            last_rs_handle: None,
            statement_cache: LruCache::new(opts.statement_cache_capacity),
            log_settings: LogSettings::default(),
            session_info,
        };

        Ok(con)
    }

    #[allow(clippy::needless_lifetimes)]
    pub(crate) async fn execute_string_query<'a>(
        &'a mut self,
        query: String,
    ) -> Result<ExaQueryResult, SqlxError> {
        self.execute_plain(&query, fetcher_closure!('a))
            .await?
            .try_filter_map(|step| async move { Ok(step.map_left(Some).left_or(None)) })
            .try_collect()
            .await
    }

    async fn execute_query<'a, C, F>(
        &'a mut self,
        sql: &str,
        arguments: Option<ExaArguments>,
        persist: bool,
        future_maker: C,
    ) -> Result<QueryResultStream<'a, C, F>, SqlxError>
    where
        C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    {
        match arguments {
            Some(args) => {
                self.execute_prepared(sql, args, persist, future_maker)
                    .await
            }
            None => self.execute_plain(sql, future_maker).await,
        }
    }

    async fn execute_prepared<'a, C, F>(
        &'a mut self,
        sql: &str,
        args: ExaArguments,
        persist: bool,
        future_maker: C,
    ) -> Result<QueryResultStream<'a, C, F>, SqlxError>
    where
        C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    {
        let prepared = self
            .ws
            .get_or_prepare(&mut self.statement_cache, sql, persist)
            .await?;

        // Check the compatibility between provided parameter data types
        // and the ones expected by the database.
        for (expected, provided) in iter::zip(prepared.parameters.as_ref(), &args.types) {
            if !expected.compatible(provided) {
                Err(ExaProtocolError::DatatypeMismatch(
                    expected.to_string(),
                    provided.to_string(),
                ))?;
            }
        }

        let cmd = ExaCommand::new_execute_prepared(
            prepared.statement_handle,
            &prepared.parameters,
            args.buf,
            &self.ws.attributes,
        )?
        .try_into()?;

        self.ws
            .get_result_stream(cmd, &mut self.last_rs_handle, future_maker)
            .await
    }

    async fn execute_plain<'a, C, F>(
        &'a mut self,
        sql: &str,
        future_maker: C,
    ) -> Result<QueryResultStream<'a, C, F>, SqlxError>
    where
        C: Fn(&'a mut ExaWebSocket, u16, usize) -> Result<F, SqlxError>,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    {
        let cmd = ExaCommand::new_execute(sql, &self.ws.attributes).try_into()?;
        self.ws
            .get_result_stream(cmd, &mut self.last_rs_handle, future_maker)
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
        Transaction::begin(self)
    }

    fn shrink_buffers(&mut self) {}

    fn flush(&mut self) -> futures_util::future::BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(self.ws.rollback())
    }

    fn should_flush(&self) -> bool {
        self.ws.pending_rollback
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
                self.ws.close_prepared(prep.statement_handle).await?
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

            assert!(conn.ws.pending_rollback);
        }

        // Should be plenty of time to execute the flush
        // after the connection is dropped.
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!pool.acquire().await?.ws.pending_rollback);

        Ok(())
    }
}
