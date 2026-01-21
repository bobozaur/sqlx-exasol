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
pub enum ReaderStream {
    RecvStream(RecvFut<'static, (ExportDataReceiver, OneShotServer<ExportService>)>),
    StreamData {
        stream: ExportDataReceiver,
        conn: Option<OneShotServer<ExportService>>,
    },
    Respond(OneShotServer<ExportService>),
}

impl Stream for ReaderStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ReaderStream::RecvStream(recv_fut) => {
                    let (stream, conn) =
                        ready!(recv_fut.poll_unpin(cx)).map_err(worker_bootstrap_error)?;

                    Self::StreamData {
                        stream,
                        conn: Some(conn),
                    }
                }
                ReaderStream::StreamData { stream, conn } => {
                    if let Poll::Ready(opt) = stream.poll_next_unpin(cx) {
                        return Poll::Ready(opt.map(Ok));
                    }
                    ready!(conn.as_mut().unwrap().poll_unpin(cx)).map_err(map_hyper_err)?;
                    Self::Respond(conn.take().unwrap())
                }
                ReaderStream::Respond(conn) => {
                    ready!(conn.poll_unpin(cx)).map_err(map_hyper_err)?;
                    return Poll::Ready(None);
                }
            };

            self.set(state);
        }
    }
}
