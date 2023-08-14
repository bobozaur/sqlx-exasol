use std::borrow::Cow;

use either::Either;
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

use super::{stream::ResultStream, websocket::ExaWebSocket};

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

        // This closure is for defining the concrete types that we need to pass around for the generics.
        //
        // What we're really interested in is defining the `F` future for retrieving the next data chunk in the result set.
        //
        // However, we in fact need a factory of these `F` future types, which is the closure here, aka the future maker.
        // This is because we will generally have to retrieve chunks until a result set is depleted, so we need
        // the ability to create new ones.
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
    ) -> futures_util::future::BoxFuture<'e, Result<Describe<Self::Database>, SqlxError>>
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
