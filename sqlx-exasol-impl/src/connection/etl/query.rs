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

/// Future for awaiting an ETL query alongside the worker I/O.
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

        if let Some(Err(_)) = Pin::new(&mut self.query_future).output_mut() {
            self.stop_tasks.store(true, Ordering::Release);
            for task in &mut self.server_tasks {
                task.waker.wake();
                ready!(task.poll_unpin(cx)).ok();
            }
        }

        match Pin::new(&mut self.query_future).take_output() {
            Some(res) => Poll::Ready(res),
            None => Poll::Pending,
        }
    }
}

/// Ensures background tasks are signaled to stop if the future
/// is not awaited to completion.
impl Drop for EtlQuery<'_> {
    fn drop(&mut self) {
        self.stop_tasks.store(true, Ordering::Release);
        for task in &self.server_tasks {
            task.waker.wake();
        }
    }
}

/// Implementor of [`WebSocketFuture`] that executes an owned ETL query.
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
