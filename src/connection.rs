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
    command::{Command, ExaCommand},
    database::Exasol,
    options::ExaConnectOptions,
    responses::{DataChunk, ExaAttributes, PreparedStatement, SessionInfo},
    statement::{ExaStatement, ExaStatementMetadata},
    stream::{QueryResultStream, ResultStream},
    websocket::{ExaWebSocket, WithRwSocket},
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
    pub fn attributes(&self) -> &ExaAttributes {
        &self.ws.attributes
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

    #[cfg(feature = "migrate")]
    pub(crate) async fn begin_transaction(&mut self) -> Result<(), SqlxError> {
        self.ws.begin().await
    }

    #[cfg(feature = "migrate")]
    pub(crate) async fn rollback_transaction(&mut self) -> Result<(), SqlxError> {
        self.ws.rollback().await
    }

    #[cfg(feature = "migrate")]
    pub(crate) async fn execute_batch(&mut self, sql: &str) -> Result<(), SqlxError> {
        self.ws.execute_batch(sql).await
    }

    async fn execute_query<'a, C, F>(
        &'a mut self,
        sql: &str,
        arguments: Option<ExaArguments>,
        persist: bool,
        fetcher_maker: C,
    ) -> Result<QueryResultStream<'a, C, F>, SqlxError>
    where
        C: FnMut(&'a mut ExaWebSocket, Command) -> F,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    {
        match arguments {
            Some(args) => {
                self.execute_prepared(sql, args, persist, fetcher_maker)
                    .await
            }
            None => self.execute_plain(sql, fetcher_maker).await,
        }
    }

    async fn execute_prepared<'a, C, F>(
        &'a mut self,
        sql: &str,
        args: ExaArguments,
        persist: bool,
        fetcher_maker: C,
    ) -> Result<QueryResultStream<'a, C, F>, SqlxError>
    where
        C: FnMut(&'a mut ExaWebSocket, Command) -> F,
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
            args.values,
            &self.ws.attributes,
        )
        .try_into()?;

        self.ws
            .get_results_stream(cmd, &mut self.last_rs_handle, fetcher_maker)
            .await
    }

    async fn execute_plain<'a, C, F>(
        &'a mut self,
        sql: &str,
        fetcher_maker: C,
    ) -> Result<QueryResultStream<'a, C, F>, SqlxError>
    where
        C: FnMut(&'a mut ExaWebSocket, Command) -> F,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), SqlxError>> + 'a,
    {
        let cmd = ExaCommand::new_execute(sql, &self.ws.attributes).try_into()?;
        self.ws
            .get_results_stream(cmd, &mut self.last_rs_handle, fetcher_maker)
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
        let future = self.execute_query(sql, arguments, persistent, ExaWebSocket::fetch_chunk);
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
