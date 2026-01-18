use std::{
    fmt::Debug,
    future::Future,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{ready, Context, Poll},
};

use futures_util::{
    future::{maybe_done, MaybeDone},
    FutureExt,
};

use crate::{
    connection::websocket::{
        future::{ExaFuture, ExaRoundtrip, WebSocketFuture},
        request::Execute,
        ExaWebSocket,
    },
    etl::{error::ExaEtlError, server::ServerTask},
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
#[derive(Debug)]
pub struct EtlQuery<'c> {
    query_future: MaybeDone<ExaFuture<'c, ExecuteEtl>>,
    server_tasks: Vec<ServerTask>,
    stop_tasks: Arc<AtomicBool>,
}

impl<'c> EtlQuery<'c> {
    pub(crate) fn new(
        query_future: ExaFuture<'c, ExecuteEtl>,
        server_tasks: Vec<ServerTask>,
        stop_tasks: Arc<AtomicBool>,
    ) -> Self {
        Self {
            query_future: maybe_done(query_future),
            server_tasks,
            stop_tasks,
        }
    }
}

impl Future for EtlQuery<'_> {
    type Output = SqlxResult<ExaQueryResult>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(self.query_future.poll_unpin(cx));

        // If the query errors out, signal the HTTP server tasks to stop and wake them.
        if let Some(Err(_)) = Pin::new(&mut self.query_future).output_mut() {
            self.stop_tasks.store(true, Ordering::Release);
            for task in &mut self.server_tasks {
                task.wake();
            }
        }

        // Query future completed, wait for the HTTP server tasks as well.
        for task in &mut self.server_tasks {
            ready!(task.poll_unpin(cx));
        }

        // Query errors always have priority.
        let Some(qr) = Pin::new(&mut self.query_future).take_output().transpose()? else {
            return Poll::Pending;
        };

        // Check for errors in the server tasks.
        //
        // NOTE: Not particularly relevant because I can't picture a reason why the servers
        //       would error out and the query would not, but added for completion.
        self.server_tasks
            .drain(..)
            .try_for_each(ServerTask::take_output)?;

        Poll::Ready(Ok(qr))
    }
}

/// Ensures background server tasks are signaled to stop if the future is not awaited to completion.
/// This prevents the tasks from running indefinitely in the background.
impl Drop for EtlQuery<'_> {
    fn drop(&mut self) {
        self.stop_tasks.store(true, Ordering::Release);
        for task in &self.server_tasks {
            task.wake();
        }
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
