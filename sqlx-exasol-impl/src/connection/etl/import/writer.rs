use std::{
    fmt, io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use flume::r#async::RecvFut;
use futures_io::{AsyncWrite, IoSlice};
use futures_util::{FutureExt, SinkExt};
use sqlx_core::bytes::BytesMut;

use crate::etl::{
    error::{broken_pipe_error, map_hyper_err, worker_bootstrap_error},
    import::{service::ImportService, ImportDataSender, ImportPartsReceiver},
    job::OneShotServer,
};

/// The inner [`AsyncWrite`] implementation for an `IMPORT` worker.
///
/// This enum represents the state machine for the writer. It starts in the [`ExaWriter::RecvParts`]
/// state, waiting for the data channel and HTTP connection. Once received, it transitions to the
/// [`ExaWriter::SinkData`] state, which handles the actual writing of data.
#[derive(Debug)]
pub enum ExaWriter {
    RecvParts {
        recv: RecvFut<'static, (ImportDataSender, OneShotServer<ImportService>)>,
        buffer_size: usize,
    },
    SinkData(ExaWriterInner),
}

impl ExaWriter {
    pub fn new(rx: ImportPartsReceiver, buffer_size: usize) -> Self {
        Self::RecvParts {
            recv: rx.into_recv_async(),
            buffer_size,
        }
    }
}

impl AsyncWrite for ExaWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ExaWriter::RecvParts { recv, buffer_size } => {
                    let (sink, conn) =
                        ready!(recv.poll_unpin(cx)).map_err(worker_bootstrap_error)?;

                    Self::SinkData(ExaWriterInner {
                        buffer: BytesMut::with_capacity(*buffer_size),
                        buffer_size: *buffer_size,
                        sink,
                        conn,
                    })
                }
                ExaWriter::SinkData(inner) => return Pin::new(inner).poll_write(cx, buf),
            };

            self.set(state);
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ExaWriter::RecvParts { recv, buffer_size } => {
                    let (sink, conn) =
                        ready!(recv.poll_unpin(cx)).map_err(worker_bootstrap_error)?;

                    Self::SinkData(ExaWriterInner {
                        buffer: BytesMut::with_capacity(*buffer_size),
                        buffer_size: *buffer_size,
                        sink,
                        conn,
                    })
                }
                ExaWriter::SinkData(inner) => return Pin::new(inner).poll_write_vectored(cx, bufs),
            };

            self.set(state);
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ExaWriter::RecvParts { recv, buffer_size } => {
                    let (sink, conn) =
                        ready!(recv.poll_unpin(cx)).map_err(worker_bootstrap_error)?;

                    Self::SinkData(ExaWriterInner {
                        buffer: BytesMut::with_capacity(*buffer_size),
                        buffer_size: *buffer_size,
                        sink,
                        conn,
                    })
                }
                ExaWriter::SinkData(inner) => return Pin::new(inner).poll_flush(cx),
            };

            self.set(state);
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let state = match self.as_mut().get_mut() {
                ExaWriter::RecvParts { recv, buffer_size } => {
                    let (sink, conn) =
                        ready!(recv.poll_unpin(cx)).map_err(worker_bootstrap_error)?;

                    Self::SinkData(ExaWriterInner {
                        buffer: BytesMut::with_capacity(*buffer_size),
                        buffer_size: *buffer_size,
                        sink,
                        conn,
                    })
                }
                ExaWriter::SinkData(inner) => return Pin::new(inner).poll_close(cx),
            };

            self.set(state);
        }
    }
}

/// The active state of the [`ExaWriter`].
///
/// This struct contains the buffer for outgoing data, the sink to send the data to the HTTP
/// service, and the HTTP connection itself.
///
/// The [`AsyncWrite`] implementation for this struct will buffer data and send it to the sink.
/// It is also responsible for polling the `conn` future to completion to ensure the HTTP
/// transaction is properly finished.
pub struct ExaWriterInner {
    buffer: BytesMut,
    buffer_size: usize,
    sink: ImportDataSender,
    conn: OneShotServer<ImportService>,
}

impl ExaWriterInner {
    fn take_buf(&mut self, take: bool) -> BytesMut {
        if take {
            std::mem::take(&mut self.buffer)
        } else {
            let buf = BytesMut::with_capacity(self.buffer_size);
            std::mem::replace(&mut self.buffer, buf)
        }
    }

    fn poll_flush_inner(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        is_end: bool,
    ) -> Poll<io::Result<()>> {
        // We must poll the connection to check if it has been closed by the server.
        // If the server closes the connection prematurely, this will return an error.
        let _ = self.conn.poll_unpin(cx).map_err(map_hyper_err)?;

        if !self.buffer.is_empty() {
            ready!(self.sink.poll_ready_unpin(cx)).map_err(map_send_error)?;
            let buffer = self.take_buf(is_end);
            self.sink.start_send_unpin(buffer).map_err(map_send_error)?;
        }

        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ExaWriterInner {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.buffer_size == self.buffer.len() {
            ready!(self.as_mut().poll_flush(cx))?;
        }

        let avail = self.buffer_size - self.buffer.len();
        let len = buf.len().min(avail);
        self.buffer.extend_from_slice(&buf[..len]);
        Poll::Ready(Ok(len))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if self.buffer_size == self.buffer.len() {
            ready!(self.as_mut().poll_flush(cx))?;
        }

        let avail = self.buffer_size - self.buffer.len();
        let mut rem = avail;
        for buf in bufs {
            if rem == 0 {
                break;
            }

            let len = buf.len().min(rem);
            self.buffer.extend_from_slice(&buf[..len]);
            rem -= len;
        }

        Poll::Ready(Ok(avail - rem))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush_inner(cx, false)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // First, flush any remaining data in the buffer and close the data sink.
        if !self.sink.is_closed() {
            ready!(self.as_mut().poll_flush_inner(cx, true))?;
            ready!(self.sink.poll_close_unpin(cx)).map_err(map_send_error)?;
        }

        // After the sink is closed, the HTTP service will send the response.
        // We must poll the connection to completion.
        ready!(self.conn.poll_unpin(cx)).map_err(map_hyper_err)?;
        Poll::Ready(Ok(()))
    }
}

impl fmt::Debug for ExaWriterInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ExaWriterInner {
            conn, buffer_size, ..
        } = self;
        f.debug_struct("ExaWriterInner")
            .field("conn", &conn)
            .field("buffer_size", &buffer_size)
            .finish()
    }
}

fn map_send_error<E>(_: E) -> io::Error {
    broken_pipe_error("error sending data from IMPORT worker to the HTTP server")
}
