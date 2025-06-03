use std::{borrow::Cow, future::ready};

use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use sqlx_core::{
    database::Database,
    describe::Describe,
    executor::{Execute, Executor},
    logger::QueryLogger,
    Either,
};

use super::stream::ResultStream;
use crate::{
    connection::websocket::future::{
        self, ExecuteBatch, ExecutePrepared, GetOrPrepare, WebSocketFuture,
    },
    database::Exasol,
    responses::DescribeStatement,
    statement::{ExaStatement, ExaStatementMetadata},
    ExaConnection, SqlxError, SqlxResult,
};

impl<'c> Executor<'c> for &'c mut ExaConnection {
    type Database = Exasol;

    fn execute<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxFuture<'e, SqlxResult<<Self::Database as Database>::QueryResult>>
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        let sql = query.sql();
        let persist = query.persistent();
        let logger = QueryLogger::new(sql, self.log_settings.clone());
        let arguments = match query.take_arguments().map_err(SqlxError::Encode) {
            Ok(a) => a,
            Err(e) => return Box::pin(ready(Err(e))),
        };

        let filter_fn = |step| async move {
            Ok(match step {
                Either::Left(rows) => Some(rows),
                Either::Right(_) => None,
            })
        };

        if let Some(arguments) = arguments {
            let future = ExecutePrepared::new(sql, persist, arguments);
            ResultStream::new(&mut self.ws, logger, future)
                .try_filter_map(filter_fn)
                .try_collect()
                .boxed()
        } else {
            let future = future::Execute::new(sql);
            ResultStream::new(&mut self.ws, logger, future)
                .try_filter_map(filter_fn)
                .try_collect()
                .boxed()
        }
    }

    fn execute_many<'e, 'q, E>(
        self,
        query: E,
    ) -> BoxStream<'e, SqlxResult<<Self::Database as Database>::QueryResult>>
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        self.fetch_many(query)
            .try_filter_map(|step| async move {
                Ok(match step {
                    Either::Left(rows) => Some(rows),
                    Either::Right(_) => None,
                })
            })
            .boxed()
    }

    fn fetch<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, SqlxResult<<Self::Database as Database>::Row>>
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        let sql = query.sql();
        let persist = query.persistent();
        let logger = QueryLogger::new(sql, self.log_settings.clone());
        let arguments = match query.take_arguments().map_err(SqlxError::Encode) {
            Ok(a) => a,
            Err(e) => return Box::pin(ready(Err(e)).into_stream()),
        };

        let filter_fn = |step| async move {
            Ok(match step {
                Either::Left(_) => None,
                Either::Right(row) => Some(row),
            })
        };

        if let Some(arguments) = arguments {
            let future = ExecutePrepared::new(sql, persist, arguments);
            Box::pin(ResultStream::new(&mut self.ws, logger, future).try_filter_map(filter_fn))
        } else {
            let future = future::Execute::new(sql);
            Box::pin(ResultStream::new(&mut self.ws, logger, future).try_filter_map(filter_fn))
        }
    }

    fn fetch_many<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxStream<
        'e,
        SqlxResult<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
        >,
    >
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        let sql = query.sql();
        let persist = query.persistent();
        let logger = QueryLogger::new(sql, self.log_settings.clone());
        let arguments = match query.take_arguments().map_err(SqlxError::Encode) {
            Ok(a) => a,
            Err(e) => return Box::pin(ready(Err(e)).into_stream()),
        };

        if let Some(arguments) = arguments {
            let future = ExecutePrepared::new(sql, persist, arguments);
            Box::pin(ResultStream::new(&mut self.ws, logger, future))
        } else {
            let future = ExecuteBatch::new(sql);
            Box::pin(ResultStream::new(&mut self.ws, logger, future))
        }
    }

    fn fetch_optional<'e, 'q, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, SqlxResult<Option<<Self::Database as Database>::Row>>>
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
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

    fn prepare_with<'e, 'q>(
        self,
        sql: &'q str,
        _parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, SqlxResult<<Self::Database as Database>::Statement<'q>>>
    where
        'q: 'e,
        'c: 'e,
    {
        Box::pin(async move {
            let prepared = GetOrPrepare::new(sql, true).future(&mut self.ws).await?;

            Ok(ExaStatement {
                sql: Cow::Borrowed(sql),
                metadata: ExaStatementMetadata::new(
                    prepared.columns.clone(),
                    prepared.parameters.clone(),
                ),
            })
        })
    }

    /// Exasol does not provide nullability information, unfortunately.
    fn describe<'e, 'q>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, SqlxResult<Describe<Self::Database>>>
    where
        'q: 'e,
        'c: 'e,
    {
        Box::pin(async move {
            let DescribeStatement {
                columns,
                parameters,
                ..
            } = future::Describe::new(sql).future(&mut self.ws).await?;

            let nullable = (0..columns.len()).map(|_| None).collect();

            Ok(Describe {
                parameters: Some(Either::Left(parameters)),
                columns,
                nullable,
            })
        })
    }
}
