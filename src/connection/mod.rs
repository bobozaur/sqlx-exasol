mod stream;
mod tls;
mod websocket;

use std::{borrow::Cow, future, iter};

use either::Either;
use lru::LruCache;
use sqlx_core::{
    column::Column,
    connection::{Connection, LogSettings},
    database::{Database, HasStatement},
    describe::Describe,
    executor::{Execute, Executor},
    logger::QueryLogger,
    transaction::Transaction,
    Error as SqlxError,
};

use futures_util::{Future, TryStreamExt};

use crate::{
    arguments::ExaArguments,
    command::ExaCommand,
    database::Exasol,
    options::ExaConnectOptions,
    responses::{DataChunk, ExaAttributes, PreparedStatement, SessionInfo},
    statement::{ExaStatement, ExaStatementMetadata},
};

use stream::{QueryResultStream, ResultStream};
use websocket::{ExaWebSocket, WithRwSocket};

#[derive(Debug)]
pub struct ExaConnection {
    pub(crate) ws: ExaWebSocket,
    pub(crate) last_rs_handle: Option<u16>,
    session_info: SessionInfo,
    statement_cache: LruCache<String, PreparedStatement>,
    log_settings: LogSettings,
}

impl ExaConnection {
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
            let socket_res = sqlx_core::net::connect_tcp(host, opts.port, WithRwSocket).await;

            let socket = match socket_res {
                Ok(socket) => socket,
                Err(err) => {
                    ws_result = Err(err);
                    continue;
                }
            };

            match ExaWebSocket::new(host, socket, opts.into()).await {
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

        let column_types = prepared.columns.iter().map(Column::type_info);
        for (expected, provided) in iter::zip(column_types, &args.types) {
            expected.check_compatibility(provided)?;
        }

        let cmd = ExaCommand::new_execute_prepared(
            prepared.statement_handle,
            &prepared.columns,
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
        Box::pin(future::ready(Ok(())))
    }

    fn should_flush(&self) -> bool {
        false
    }
}

impl<'c> Executor<'c> for &'c mut ExaConnection {
    type Database = Exasol;

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        mut query: E,
    ) -> futures_util::stream::BoxStream<
        'e,
        Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            SqlxError,
        >,
    >
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
    {
        let sql = query.sql();
        let arguments = query.take_arguments();
        let persistent = query.persistent();

        let logger = QueryLogger::new(sql, self.log_settings.clone());

        // This closure is a fairly short way of defining the concrete types that we need to pass around
        // for the generics.
        //
        // What we're really interested in is defining the `F` future for retrieving the next data chunk in the result set.
        //
        // However, we in fact need a factory of these `F` future types, which is the closure here, aka the future maker.
        // This is because we will generally have to retrieve chunks until a result set is depleted.
        //
        // Since the future uses the exclusive mutable reference to the websocket, to satisfy the borrow checker
        // we return the mutable reference after the future is done it with, so it can be passed to the future maker again
        // and create a new future.
        let future_maker = move |ws: &'e mut ExaWebSocket, handle: u16, pos: usize| {
            let fetch_size = ws.attributes.fetch_size;
            let cmd = ExaCommand::new_fetch(handle, pos, fetch_size).try_into()?;
            let future = async { ws.fetch_chunk(cmd).await.map(|d| (d, ws)) };
            Ok(future)
        };

        let future = self.execute_query(sql, arguments, persistent, future_maker);
        Box::pin(ResultStream::new(future, logger))
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> futures_util::future::BoxFuture<
        'e,
        Result<Option<<Self::Database as Database>::Row>, SqlxError>,
    >
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
    {
        let mut s = self.fetch_many(query);

        Box::pin(async move {
            while let Some(v) = s.try_next().await? {
                if let Either::Right(r) = v {
                    return Ok(Some(r));
                }
            }

            Ok(None)
        })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        _parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> futures_util::future::BoxFuture<
        'e,
        Result<<Self::Database as HasStatement<'q>>::Statement, SqlxError>,
    >
    where
        'c: 'e,
    {
        Box::pin(async move {
            let prepared = self
                .ws
                .get_or_prepare(&mut self.statement_cache, sql, true)
                .await?;

            Ok(ExaStatement {
                sql: Cow::Borrowed(sql),
                metadata: ExaStatementMetadata::new(prepared.columns.clone()),
            })
        })
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures_util::future::BoxFuture<'e, Result<Describe<Self::Database>, SqlxError>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let cmd = ExaCommand::new_create_prepared(sql).try_into()?;

            let PreparedStatement {
                statement_handle,
                columns,
            } = self.ws.create_prepared(cmd).await?;

            self.ws.close_prepared(statement_handle).await?;

            let mut nullable = Vec::with_capacity(columns.len());
            let mut parameters = Vec::with_capacity(columns.len());

            for column in columns.as_ref() {
                nullable.push(None);
                parameters.push(column.datatype.clone())
            }

            let columns = columns.iter().map(ToOwned::to_owned).collect();

            Ok(Describe {
                parameters: Some(Either::Left(parameters)),
                columns,
                nullable,
            })
        })
    }
}

#[cfg(test)]
#[cfg(feature = "migrate")]
mod tests {
    use std::num::NonZeroUsize;

    use sqlx::query;
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
}
