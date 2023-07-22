use std::{borrow::Cow, future, num::NonZeroUsize};

use either::Either;
use lru::LruCache;
use sqlx_core::{
    connection::Connection,
    database::{Database, HasStatement},
    describe::Describe,
    executor::{Execute, Executor},
    transaction::Transaction,
    Error as SqlxError,
};

use futures_util::{Future, TryStreamExt};

use crate::{
    arguments::ExaArguments,
    command::{Command, ExecutePreparedStmt, Fetch, SqlText},
    con_opts::ExaConnectOptions,
    database::Exasol,
    responses::{fetched::DataChunk, prepared_stmt::PreparedStatement},
    statement::{ExaStatement, ExaStatementMetadata},
    stream::{QueryResultStream, ResultStream},
    websocket::ExaWebSocket,
};

#[derive(Debug)]
pub struct ExaConnection {
    pub(crate) ws: ExaWebSocket,
    // use_compression: bool,
    pub(crate) last_result_set_handle: Option<u16>,
    stmt_cache: LruCache<String, PreparedStatement>,
}

impl ExaConnection {
    pub async fn new(ws: ExaWebSocket) -> Self {
        Self {
            ws,
            // use_compression: false,
            last_result_set_handle: None,
            stmt_cache: LruCache::new(NonZeroUsize::new(10).unwrap()),
        }
    }

    async fn execute_query<'a, C, F>(
        &'a mut self,
        sql: &str,
        arguments: Option<ExaArguments>,
        persistent: bool,
        fetch_maker: C,
    ) -> Result<QueryResultStream<'a, C, F>, String>
    where
        C: FnMut(&'a mut ExaWebSocket, Fetch) -> F,
        F: Future<Output = Result<(DataChunk, &'a mut ExaWebSocket), String>> + 'a,
    {
        if let Some(arguments) = arguments {
            let prepared = self
                .ws
                .get_or_prepare(&mut self.stmt_cache, sql, persistent)
                .await?;
            let data = arguments.0.into_iter().map(|v| vec![v]).collect();
            let command =
                ExecutePreparedStmt::new(prepared.statement_handle, &prepared.columns, data);
            let command = Command::ExecutePreparedStatement(command);
            self.ws
                .get_results_stream(command, &mut self.last_result_set_handle, fetch_maker)
                .await
        } else {
            let command = Command::Execute(SqlText::new(sql));
            self.ws
                .get_results_stream(command, &mut self.last_result_set_handle, fetch_maker)
                .await
        }
    }
}

impl Connection for ExaConnection {
    type Database = Exasol;

    type Options = ExaConnectOptions;

    fn close(mut self) -> futures_util::future::BoxFuture<'static, Result<(), SqlxError>> {
        Box::pin(async move {
            self.ws.disconnect().await.map_err(SqlxError::Protocol)?;

            self.ws.close().await.map_err(SqlxError::Protocol)?;

            Ok(())
        })
    }

    fn close_hard(mut self) -> futures_util::future::BoxFuture<'static, Result<(), SqlxError>> {
        Box::pin(async move {
            self.ws.close().await.map_err(SqlxError::Protocol)?;

            Ok(())
        })
    }

    fn ping(&mut self) -> futures_util::future::BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move {
            self.ws.ping().await.map_err(SqlxError::Protocol)?;
            Ok(())
        })
    }

    fn begin(
        &mut self,
    ) -> futures_util::future::BoxFuture<'_, Result<Transaction<'_, Self::Database>, SqlxError>>
    where
        Self: Sized,
    {
        todo!()
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

        let future = self.execute_query(sql, arguments, persistent, ExaWebSocket::fetch_chunk2);
        Box::pin(ResultStream::new(future).map_err(SqlxError::Protocol))
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
                .get_or_prepare(&mut self.stmt_cache, sql, true)
                .await
                .map_err(SqlxError::Protocol)?;

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
            let command = SqlText::new(sql);
            let PreparedStatement {
                statement_handle,
                columns,
            } = self
                .ws
                .create_prepared(Command::CreatePreparedStatement(command))
                .await
                .map_err(SqlxError::Protocol)?;

            self.ws
                .close_prepared(statement_handle)
                .await
                .map_err(SqlxError::Protocol)?;

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
