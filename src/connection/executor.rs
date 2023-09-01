use std::borrow::Cow;

use either::Either;
use futures_core::{future::BoxFuture, stream::BoxStream};
use sqlx_core::{
    database::{Database, HasStatement},
    describe::Describe,
    executor::{Execute, Executor},
    logger::QueryLogger,
    Error as SqlxError,
};

use futures_util::TryStreamExt;

use crate::{
    command::ExaCommand,
    database::Exasol,
    responses::DescribeStatement,
    statement::{ExaStatement, ExaStatementMetadata},
    ExaConnection,
};

use super::{macros::fetcher_closure, stream::ResultStream};

impl<'c> Executor<'c> for &'c mut ExaConnection {
    type Database = Exasol;

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
        let arguments = query.take_arguments();
        let persistent = query.persistent();

        let logger = QueryLogger::new(sql, self.log_settings.clone());

        let future = self.execute_query(sql, arguments, persistent, fetcher_closure!('e));
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
    ) -> BoxFuture<'e, Result<<Self::Database as HasStatement<'q>>::Statement, SqlxError>>
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
