use std::{
    fmt, io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use flume::r#async::{RecvFut, RecvStream};
use futures_io::{AsyncBufRead, AsyncRead};
use futures_util::{stream::IntoAsyncRead, FutureExt, Stream, StreamExt, TryStreamExt};
use hyper::body::Bytes;

use crate::etl::{
    error::channel_pipe_error,
    export::{ExportChannelReceiver, ExportDataReceiver},
};

pub struct ExaReaderInner(IntoAsyncRead<ReaderStream>);

impl ExaReaderInner {
    pub fn new(rx: ExportChannelReceiver) -> Self {
        Self(ReaderStream::RecvStream(rx.into_recv_async()).into_async_read())
    }
}

// Not derived so that the reader field is ignored as it's mainly just a stream of chunks that
// takes a lot of space.
impl fmt::Debug for ExaReaderInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExaReader").finish()
    }
}

impl AsyncRead for ExaReaderInner {
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

impl AsyncBufRead for ExaReaderInner {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().0).poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.0).consume(amt);
    }
}

pub enum ReaderStream {
    RecvStream(RecvFut<'static, ExportDataReceiver>),
    StreamData(RecvStream<'static, Bytes>),
}

impl Stream for ReaderStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.as_mut().get_mut() {
                ReaderStream::RecvStream(recv_fut) => {
                    let stream = ready!(recv_fut.poll_unpin(cx))
                        .map_err(channel_pipe_error)?
                        .into_stream();
                    self.set(Self::StreamData(stream));
                }
                ReaderStream::StreamData(stream) => {
                    break Poll::Ready(ready!(stream.poll_next_unpin(cx)).map(Ok))
                }
            }
        }
    }
}
