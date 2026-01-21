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
/// This future polls both the main query future and the background HTTP server tasks.
/// It ensures that all server tasks are completed before returning the result of the query.
///
/// If the [`EtlQuery`] future is dropped before completion, it will signal the server tasks to
/// stop, preventing them from running indefinitely.
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
        if !self.query.is_terminated() && self.query.poll_unpin(cx).is_ready() {
            if let Some(query_res) = Pin::new(&mut self.query).take_output() {
                return Poll::Ready(query_res);
            }
        }

        if !self.bootstrap.is_terminated() && self.bootstrap.poll_unpin(cx).is_ready() {
            Pin::new(&mut self.bootstrap).take_output().transpose()?;
        }

        Poll::Pending
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
