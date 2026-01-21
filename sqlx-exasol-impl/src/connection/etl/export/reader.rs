use std::{
    fmt, io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use flume::r#async::RecvFut;
use futures_io::{AsyncBufRead, AsyncRead};
use futures_util::{stream::IntoAsyncRead, FutureExt, Stream, StreamExt, TryStreamExt};
use hyper::body::Bytes;

use crate::etl::{
    error::{map_hyper_err, worker_bootstrap_error},
    export::{service::ExportService, ExportDataReceiver, ExportPartsReceiver},
    job::OneShotServer,
};

/// The inner `AsyncRead` implementation for an `EXPORT` worker.
///
/// This struct wraps a [`ReaderStream`] and provides the [`AsyncRead`] and [`AsyncBufRead`]
/// implementations that are then used by [`crate::connection::etl::ExaExport`].
pub struct ExaReader(IntoAsyncRead<ReaderStream>);

impl ExaReader {
    pub fn new(rx: ExportPartsReceiver) -> Self {
        Self(ReaderStream::RecvStream(rx.into_recv_async()).into_async_read())
    }
}

impl fmt::Debug for ExaReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExaReader").finish()
    }
}

impl AsyncRead for ExaReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_read_vectored(cx, bufs)
    }
}

impl AsyncBufRead for ExaReader {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().0).poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.0).consume(amt);
    }
}

/// A stream that receives data for an `EXPORT` worker.
///
/// This enum represents the state machine of the export worker's data stream.
///
/// 1. [`ReaderStream::RecvStream`]: The initial state. The stream is waiting to receive the data
///    channel and the HTTP connection from the server bootstrap process.
///
/// 2. [`ReaderStream::StreamData`]: The active data transfer state. The stream yields data chunks
///    received from the HTTP service. In this state, it also polls the connection future. Polling
///    will continue until the entire HTTP request is processed and the response is sent.
pub enum ReaderStream {
    RecvStream(RecvFut<'static, (ExportDataReceiver, OneShotServer<ExportService>)>),
    StreamData {
        stream: ExportDataReceiver,
        conn: OneShotServer<ExportService>,
    },
}

impl Stream for ReaderStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = match self.as_mut().get_mut() {
                // Initial state: Wait to receive the data channel and HTTP connection.
                ReaderStream::RecvStream(recv_fut) => {
                    let (stream, conn) =
                        ready!(recv_fut.poll_unpin(cx)).map_err(worker_bootstrap_error)?;

                    Self::StreamData { stream, conn }
                }
                // Active state: Stream data chunks and poll the HTTP connection.
                ReaderStream::StreamData { stream, conn } => {
                    // Poll for the next data chunk.
                    if let Poll::Ready(Some(data)) = stream.poll_next_unpin(cx) {
                        return Poll::Ready(Some(Ok(data)));
                    }

                    // The request is either pending or depleted.
                    // Poll the connection to continue processing the request or respond.
                    ready!(conn.poll_unpin(cx)).map_err(map_hyper_err)?;

                    // If we are here, we responded to Exasol and the connection completed.
                    return Poll::Ready(None);
                }
            };

            self.set(state);
        }
    }
}
