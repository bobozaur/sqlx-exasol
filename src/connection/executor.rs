use std::{borrow::Cow, future::ready};

use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::{FutureExt, StreamExt, TryStreamExt};
use sqlx_core::{
    database::Database,
    describe::Describe,
    executor::{Execute, Executor},
    logger::QueryLogger,
    Either, Error as SqlxError,
};

use super::stream::ResultStream;
use crate::{
    command::ExaCommand,
    database::Exasol,
    responses::DescribeStatement,
    statement::{ExaStatement, ExaStatementMetadata},
    ExaConnection,
};

#[allow(clippy::multiple_bound_locations)]
impl<'c> Executor<'c> for &'c mut ExaConnection {
    type Database = Exasol;

    fn execute<'e, 'q: 'e, E>(
        self,
        _query: E,
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::QueryResult, SqlxError>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        // self.fetch(query).try_collect().boxed()
        todo!()
    }

    fn execute_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<<Self::Database as Database>::QueryResult, SqlxError>>
    where
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

    fn fetch<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<<Self::Database as Database>::Row, SqlxError>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        let sql = query.sql();
        let persistent = query.persistent();
        let arguments = match query.take_arguments().map_err(SqlxError::Encode) {
            Ok(a) => a,
            Err(e) => return Box::pin(ready(Err(e)).into_stream()),
        };

        let logger = QueryLogger::new(sql, self.log_settings.clone());
        let future = self.execute_query(sql, arguments, persistent);
        Box::pin(
            ResultStream::new(future, logger).try_filter_map(|step| async move {
                Ok(match step {
                    Either::Left(_) => None,
                    Either::Right(row) => Some(row),
                })
            }),
        )
    }

    fn fetch_many<'e, 'q: 'e, E: 'q>(
        self,
        mut query: E,
    ) -> BoxStream<
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
        let persistent = query.persistent();
        let arguments = match query.take_arguments().map_err(SqlxError::Encode) {
            Ok(a) => a,
            Err(e) => return Box::pin(ready(Err(e)).into_stream()),
        };

        let logger = QueryLogger::new(sql, self.log_settings.clone());
        let future = self.execute_query(sql, arguments, persistent);
        Box::pin(ResultStream::new(future, logger))
    }

    fn fetch_optional<'e, 'q: 'e, E: 'q>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<Self::Database as Database>::Row>, SqlxError>>
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
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement<'q>, SqlxError>>
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
                metadata: ExaStatementMetadata::new(
                    prepared.columns.clone(),
                    prepared.parameters.clone(),
                ),
            })
        })
    }

    /// Exasol does not provide nullability information, unfortunately.
    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, SqlxError>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let cmd = ExaCommand::new_create_prepared(sql).try_into()?;

            let DescribeStatement {
                columns,
                parameters,
                statement_handle,
            } = self.ws.describe(cmd).await?;

            self.ws.close_prepared(statement_handle).await?;

            let nullable = (0..columns.len()).map(|_| None).collect();

            Ok(Describe {
                parameters: Some(Either::Left(parameters)),
                columns,
                nullable,
            })
        })
    }
}
