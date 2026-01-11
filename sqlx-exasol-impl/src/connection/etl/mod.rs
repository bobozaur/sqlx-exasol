//! This module provides the building blocks for creating IMPORT and EXPORT jobs.
//! These are represented by a query that gets executed concurrently with some ETL workers, both of
//! which are obtained by building the ETL job. The data format is always CSV, but there are some
//! customizations that can be done on the builders such as row or column separator, etc.
//!
//! The query execution is driven by a future obtained from building the job, and will rely
//! on using the workers (also obtained from building the job) to complete and return. The
//! results is of type [`ExaQueryResult`] which can give you the number of affected rows.
//!
//! IMPORT jobs are constructed through the [`ImportBuilder`] type and will generate workers of type
//! [`ExaImport`]. The workers can be used to write data to the database and the query execution
//! ends when all the workers have been closed (by explicitly calling `close().await`).
//!
//! EXPORT jobs are constructed through the [`ExportBuilder`] type and will generate workers of type
//! [`ExaExport`]. The workers can be used to read data from the database and the query execution
//! ends when all the workers receive EOF. They can be dropped afterwards.
//!
//! ETL jobs can use TLS, compression, or both and will do so in a
//! consistent manner with the [`crate::ExaConnection`] they are executed on.
//! That means that if the connection uses TLS / compression, so will the ETL job.
//!
//! **NOTE:** Trying to run ETL jobs with TLS without an ETL TLS feature flag results
//! in a runtime error. Furthermore, enabling more than one ETL TLS feature results in a
//! compile time error.
//!
//! # Atomicity
//!
//! `IMPORT` jobs are not atomic by themselves. If an error occurs during the data ingestion,
//! some of the data might be already sent and written in the database. However, since
//! `IMPORT` is fundamentally just a query, it *can* be transactional. Therefore,
//! beginning a transaction and passing that to the [`ImportBuilder::build`] method will result in
//! the import job needing to be explicitly committed:
//!
//! ```rust,no_run
//! use std::env;
//!
//! use sqlx_exasol::{error::*, etl::*, *};
//!
//! # async {
//! #
//! let pool = ExaPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
//! let mut con = pool.acquire().await?;
//! let mut tx = con.begin().await?;
//!
//! let (query_fut, writers) = ImportBuilder::new("SOME_TABLE").build(&mut *tx).await?;
//!
//! // concurrently use the writers and await the query future
//!
//! tx.commit().await?;
//! #
//! # let res: Result<(), BoxDynError> = Ok(());
//! # res
//! # };
//! ```
//!
//! # IMPORTANT
//!
//! Exasol doesn't really like it when [`ExaImport`] workers are closed without ever sending any
//! data. The underlying socket connection to Exasol will be closed, and Exasol will just try to
//! open a new one. However, workers only listen on the designated sockets once, so the connection
//! will be refused (even if it weren't, the cycle might just repeat since we'd still be sending no
//! data).
//!
//! Therefore, it is wise not to build IMPORT jobs with more workers than required, depending on the
//! amount of data to be imported and especially if certain workers won't be written to at all.
//!
//! Additionally, Exasol expects that all [`ExaExport`] are read in their entirety (until EOF is
//! reached). Failing to do so will result in the query execution returning an error. If, for some
//! reason, you do not want to exhaust the readers, be prepared to handle the error returned by the
//! `EXPORT` query.

mod error;
mod export;
mod import;
mod job;

use std::{
    future::Future,
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};

pub use export::{ExaExport, ExportBuilder, ExportSource};
use futures_io::{AsyncRead, AsyncWrite};
use futures_util::FutureExt;
use hyper::rt;
pub use import::{ExaImport, ImportBuilder, Trim};

use self::error::ExaEtlError;
use crate::{
    connection::websocket::{
        future::{ExaFuture, ExaRoundtrip, WebSocketFuture},
        request::Execute,
        socket::ExaSocket,
        ExaWebSocket,
    },
    responses::{QueryResult, SingleResult},
    ExaQueryResult, SqlxResult,
};

// CSV row separator.
#[derive(Debug, Clone, Copy)]
pub enum RowSeparator {
    LF,
    CR,
    CRLF,
}

impl AsRef<str> for RowSeparator {
    fn as_ref(&self) -> &str {
        match self {
            RowSeparator::LF => "LF",
            RowSeparator::CR => "CR",
            RowSeparator::CRLF => "CRLF",
        }
    }
}

/// Future for awaiting an ETL query alongside the worker I/O.
#[derive(Debug)]
pub struct EtlQuery<'c>(ExaFuture<'c, ExecuteEtl>);

impl Future for EtlQuery<'_> {
    type Output = SqlxResult<ExaQueryResult>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_unpin(cx)
    }
}

/// Implementor of [`WebSocketFuture`] that executes an owned ETL query.
#[derive(Debug)]
struct ExecuteEtl(ExaRoundtrip<Execute, SingleResult>);

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

impl rt::Read for ExaSocket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        // SAFETY: The AsyncRead::poll_read call initializes and
        // fills the provided buffer. We do however need to cast it
        // to a mutable byte array first so the argument datatype matches.
        unsafe {
            let buffer: *mut [std::mem::MaybeUninit<u8>] = buf.as_mut();
            let buffer = &mut *(buffer as *mut [u8]);
            let n = ready!(AsyncRead::poll_read(self, cx, buffer))?;
            buf.advance(n);
        }

        Poll::Ready(Ok(()))
    }
}

impl rt::Write for ExaSocket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        AsyncWrite::poll_write(self, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_flush(self, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_close(self, cx)
    }
}
