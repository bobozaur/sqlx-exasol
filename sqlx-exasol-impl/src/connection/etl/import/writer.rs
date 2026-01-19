use std::{
    fmt, io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use flume::r#async::{RecvFut, SendSink};
use futures_io::{AsyncWrite, IoSlice};
use futures_util::{FutureExt, Sink, SinkExt};
use sqlx_core::bytes::BytesMut;

use crate::etl::{
    error::recv_channel_error,
    import::{ImportChannelReceiver, ImportDataSender},
};

/// The inner [`AsyncWrite`] implementation for an `IMPORT` worker.
///
/// This struct buffers data and sends it to the [`super::service::ImportService`] through a
/// channel. It is then used by [`crate::connection::etl::ExaImport`].
pub struct ExaWriter {
    buffer: BytesMut,
    buffer_size: usize,
    sink: WriterSink,
}

impl ExaWriter {
    pub fn new(rx: ImportChannelReceiver, buffer_size: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(buffer_size),
            buffer_size,
            sink: WriterSink::RecvSink(rx.into_recv_async()),
        }
    }
}

impl fmt::Debug for ExaWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ExaWriter {
            buffer_size,
            sink,
            buffer: _buffer,
        } = self;
        f.debug_struct("ExaWriter")
            .field("buffer_size", &buffer_size)
            .field("sink", &sink)
            .finish()
    }
}

impl AsyncWrite for ExaWriter {
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

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.buffer.is_empty() {
            ready!(self.sink.poll_ready_unpin(cx))?;
            let new_buffer = BytesMut::with_capacity(self.buffer_size);
            let buffer = std::mem::replace(&mut self.buffer, new_buffer);
            self.sink.start_send_unpin(buffer)?;
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.sink.is_closed() {
            if !self.buffer.is_empty() {
                ready!(self.sink.poll_ready_unpin(cx))?;
                let buffer = std::mem::take(&mut self.buffer);
                self.sink.start_send_unpin(buffer)?;
            }
            ready!(self.sink.poll_close_unpin(cx))?;
        }

        Poll::Ready(Ok(()))
    }
}

/// A sink that sends data for an `IMPORT` worker.
///
/// This enum represents the two states of the sink:
/// 1. [`WriterSink::RecvSink`]: The sink is waiting to receive the data channel from the
///    [`super::service::ImportService`].
/// 2. [`WriterSink::SinkData`]: The data channel has been received, and the sink is now sending
///    data chunks.
#[derive(Debug)]
enum WriterSink {
    RecvSink(RecvFut<'static, ImportDataSender>),
    SinkData(SendSink<'static, BytesMut>),
}

impl WriterSink {
    fn is_closed(&self) -> bool {
        match &self {
            WriterSink::RecvSink(_) => false,
            WriterSink::SinkData(sink) => sink.is_disconnected(),
        }
    }
}

impl Sink<BytesMut> for WriterSink {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match self.as_mut().get_mut() {
                WriterSink::RecvSink(recv_fut) => {
                    let sink = ready!(recv_fut.poll_unpin(cx))
                        .map_err(recv_channel_error)?
                        .into_sink();

                    self.set(WriterSink::SinkData(sink));
                }
                WriterSink::SinkData(sink) => {
                    break sink.poll_ready_unpin(cx).map_err(worker_send_data_error)
                }
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: BytesMut) -> Result<(), Self::Error> {
        match self.get_mut() {
            WriterSink::RecvSink(_) => unreachable!("start_send is always preceded by poll_ready"),
            WriterSink::SinkData(sink) => {
                sink.start_send_unpin(item).map_err(worker_send_data_error)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match self.as_mut().get_mut() {
                WriterSink::RecvSink(recv_fut) => {
                    let sink = ready!(recv_fut.poll_unpin(cx))
                        .map_err(recv_channel_error)?
                        .into_sink();

                    self.set(WriterSink::SinkData(sink));
                }
                WriterSink::SinkData(sink) => {
                    break sink.poll_flush_unpin(cx).map_err(worker_send_data_error)
                }
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match self.as_mut().get_mut() {
                WriterSink::RecvSink(recv_fut) => {
                    let sink = ready!(recv_fut.poll_unpin(cx))
                        .map_err(recv_channel_error)?
                        .into_sink();

                    self.set(WriterSink::SinkData(sink));
                }
                WriterSink::SinkData(sink) => {
                    break sink.poll_close_unpin(cx).map_err(worker_send_data_error)
                }
            }
        }
    }
}

fn worker_send_data_error<T>(_: T) -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "error sending data to the HTTP server",
    )
}
