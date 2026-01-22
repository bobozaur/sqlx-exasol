use std::{
    fmt::Debug,
    future::Future,
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures_core::FusedFuture;
use futures_util::{
    future::{self, MaybeDone, TryJoinAll},
    FutureExt,
};

use crate::{
    connection::websocket::{
        future::{ExaFuture, ExaRoundtrip, WebSocketFuture},
        request::Execute,
        ExaWebSocket,
    },
    etl::{error::ExaEtlError, job::ServerBootstrap},
    responses::{QueryResult, SingleResult},
    ExaQueryResult, SqlxResult,
};

/// A future that drives an ETL query to completion.
///
/// This future polls two main components:
/// 1. The main query future that sends the `IMPORT` or `EXPORT` statement to Exasol.
/// 2. The server bootstrap futures, which handle the connection and setup of the one-shot HTTP
///    servers for each ETL worker.
///
/// Both sets of futures are polled, but the final result of this future is the result of the
/// main database query. The bootstrap futures are expected to complete successfully much earlier
/// than the query future.
///
/// An [`EtlQuery`] is created by [`super::ImportBuilder::build`] or
/// [`super::ExportBuilder::build`].
pub struct EtlQuery<'c> {
    query: MaybeDone<ExaFuture<'c, ExecuteEtl>>,
    bootstrap: MaybeDone<TryJoinAll<ServerBootstrap>>,
}

impl<'c> EtlQuery<'c> {
    pub(crate) fn new(
        query_future: ExaFuture<'c, ExecuteEtl>,
        bootstrap_futures: Vec<ServerBootstrap>,
    ) -> Self {
        Self {
            query: future::maybe_done(query_future),
            bootstrap: future::maybe_done(future::try_join_all(bootstrap_futures)),
        }
    }
}

impl Future for EtlQuery<'_> {
    type Output = SqlxResult<ExaQueryResult>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.bootstrap.is_terminated() {
            let _ = self.bootstrap.poll_unpin(cx);
        }

        if !self.query.is_terminated() {
            let _ = self.query.poll_unpin(cx);
        }

        // Give priority to query errors
        let query_res = Pin::new(&mut self.query).take_output().transpose()?;
        Pin::new(&mut self.bootstrap).take_output().transpose()?;
        query_res.map(Ok).map_or(Poll::Pending, Poll::Ready)
    }
}

/// A [`WebSocketFuture`] implementor that executes an ETL query.
///
/// This future wrapper handles the execution of the ETL `IMPORT` or `EXPORT` query and ensures that
/// the response from Exasol is a row count, not a result set.
#[derive(Debug)]
pub struct ExecuteEtl(pub ExaRoundtrip<Execute, SingleResult>);

impl WebSocketFuture for ExecuteEtl {
    type Output = ExaQueryResult;

    fn poll_unpin(
        &mut self,
        cx: &mut Context<'_>,
        ws: &mut ExaWebSocket,
    ) -> Poll<SqlxResult<Self::Output>> {
        match QueryResult::from(ready!(self.0.poll_unpin(cx, ws))?) {
            QueryResult::ResultSet { .. } => Err(io::Error::from(ExaEtlError::ResultSetFromEtl))?,
            QueryResult::RowCount { row_count } => Poll::Ready(Ok(ExaQueryResult::new(row_count))),
        }
    }
}

impl Debug for EtlQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtlQuery").finish()
    }
}
