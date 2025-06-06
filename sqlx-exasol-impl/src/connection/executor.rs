use std::borrow::Cow;

use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use sqlx_core::{
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
    ExaConnection, ExaQueryResult, ExaRow, ExaTypeInfo, SqlxError, SqlxResult,
};

impl<'c> Executor<'c> for &'c mut ExaConnection {
    type Database = Exasol;

    fn execute<'e, 'q, E>(self, query: E) -> BoxFuture<'e, SqlxResult<ExaQueryResult>>
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Exasol>,
    {
        match self.fetch_impl(query) {
            Ok(stream) => stream
                .try_filter_map(|v| std::future::ready(Ok(v.left())))
                .try_collect()
                .boxed(),
            Err(e) => std::future::ready(Err(e)).boxed(),
        }
    }

    fn execute_many<'e, 'q, E>(self, query: E) -> BoxStream<'e, SqlxResult<ExaQueryResult>>
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Exasol>,
    {
        match self.fetch_many_impl(query) {
            Ok(stream) => stream
                .try_filter_map(|step| std::future::ready(Ok(step.left())))
                .boxed(),
            Err(e) => std::future::ready(Err(e)).into_stream().boxed(),
        }
    }

    fn fetch<'e, 'q, E>(self, query: E) -> BoxStream<'e, SqlxResult<ExaRow>>
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Exasol>,
    {
        match self.fetch_impl(query) {
            Ok(stream) => stream
                .try_filter_map(|v| std::future::ready(Ok(v.right())))
                .boxed(),
            Err(e) => std::future::ready(Err(e)).into_stream().boxed(),
        }
    }

    fn fetch_many<'e, 'q, E>(
        self,
        query: E,
    ) -> BoxStream<'e, SqlxResult<Either<ExaQueryResult, ExaRow>>>
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Exasol>,
    {
        match self.fetch_many_impl(query) {
            Ok(stream) => stream.boxed(),
            Err(e) => std::future::ready(Err(e)).into_stream().boxed(),
        }
    }

    fn fetch_all<'e, 'q: 'e, E>(self, query: E) -> BoxFuture<'e, SqlxResult<Vec<ExaRow>>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        match self.fetch_impl(query) {
            Ok(stream) => stream
                .try_filter_map(|v| std::future::ready(Ok(v.right())))
                .try_collect()
                .boxed(),
            Err(e) => std::future::ready(Err(e)).boxed(),
        }
    }

    fn fetch_one<'e, 'q: 'e, E>(self, query: E) -> BoxFuture<'e, SqlxResult<ExaRow>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        let stream = match self.fetch_impl(query) {
            Ok(stream) => stream,
            Err(e) => return std::future::ready(Err(e)).boxed(),
        };

        Box::pin(async move {
            stream
                .try_filter_map(|v| std::future::ready(Ok(v.right())))
                .try_next()
                .await
                .transpose()
                .unwrap_or(Err(SqlxError::RowNotFound))
        })
    }

    fn fetch_optional<'e, 'q, E>(self, query: E) -> BoxFuture<'e, SqlxResult<Option<ExaRow>>>
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Exasol>,
    {
        let stream = match self.fetch_impl(query) {
            Ok(stream) => stream,
            Err(e) => return std::future::ready(Err(e)).boxed(),
        };

        Box::pin(async move {
            stream
                .try_filter_map(|v| std::future::ready(Ok(v.right())))
                .try_next()
                .await
        })
    }

    fn prepare_with<'e, 'q>(
        self,
        sql: &'q str,
        _parameters: &'e [ExaTypeInfo],
    ) -> BoxFuture<'e, SqlxResult<ExaStatement<'q>>>
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
    fn describe<'e, 'q>(self, sql: &'q str) -> BoxFuture<'e, SqlxResult<Describe<Exasol>>>
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

impl ExaConnection {
    fn fetch_impl<'c, 'e, 'q, E>(&'c mut self, mut query: E) -> SqlxResult<ResultStream<'e>>
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Exasol>,
    {
        let sql = query.sql();
        let persist = query.persistent();
        let logger = QueryLogger::new(sql, self.log_settings.clone());
        let arguments = query.take_arguments().map_err(SqlxError::Encode)?;

        if let Some(arguments) = arguments {
            let future = ExecutePrepared::new(sql, persist, arguments);
            Ok(ResultStream::new(&mut self.ws, logger, future))
        } else {
            let future = future::Execute::new(sql);
            Ok(ResultStream::new(&mut self.ws, logger, future))
        }
    }

    fn fetch_many_impl<'c, 'e, 'q, E>(&'c mut self, mut query: E) -> SqlxResult<ResultStream<'e>>
    where
        'q: 'e,
        'c: 'e,
        E: 'q + Execute<'q, Exasol>,
    {
        let sql = query.sql();
        let persist = query.persistent();
        let logger = QueryLogger::new(sql, self.log_settings.clone());
        let arguments = query.take_arguments().map_err(SqlxError::Encode)?;

        if let Some(arguments) = arguments {
            let future = ExecutePrepared::new(sql, persist, arguments);
            Ok(ResultStream::new(&mut self.ws, logger, future))
        } else {
            let future = ExecuteBatch::new(sql);
            Ok(ResultStream::new(&mut self.ws, logger, future))
        }
    }
}
