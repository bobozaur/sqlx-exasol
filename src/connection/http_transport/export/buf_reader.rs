use std::{
    io::{Read, Result as IoResult},
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures_io::{AsyncBufRead, AsyncRead, IoSliceMut};
use pin_project::pin_project;

use super::reader::ExportReader;

/// Custom buffered reader, closely mimicking (as in blatantly copying)
/// [`futures_util::io::BufReader`].
///
/// The difference is that this reader will ALWAYS buffer, even if
/// the provided buffer is bigger than the internal one. This is so
/// the second difference can take place: after reading from the underlying
/// reader another read into an empty buffer takes place.
///
/// This is to ensure that the reader transitions through its states.
#[pin_project]
#[derive(Debug)]
pub struct ExportBufReader {
    #[pin]
    inner: ExportReader,
    buffer: Box<[u8]>,
    pos: usize,
    cap: usize,
}

impl ExportBufReader {
    const DEFAULT_BUF_SIZE: usize = 8 * 1024;

    pub fn new(inner: ExportReader) -> Self {
        Self {
            inner,
            buffer: vec![0; Self::DEFAULT_BUF_SIZE].into_boxed_slice(),
            pos: 0,
            cap: 0,
        }
    }
}

impl AsyncRead for ExportBufReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        let mut rem = ready!(self.as_mut().poll_fill_buf(cx))?;
        let nread = rem.read(buf)?;
        self.consume(nread);
        Poll::Ready(Ok(nread))
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<IoResult<usize>> {
        let mut rem = ready!(self.as_mut().poll_fill_buf(cx))?;
        let nread = rem.read_vectored(bufs)?;
        self.consume(nread);
        Poll::Ready(Ok(nread))
    }
}

impl AsyncBufRead for ExportBufReader {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<&[u8]>> {
        let mut this = self.project();

        // If we've reached the end of our internal buffer then we need to fetch
        // some more data from the underlying reader.
        // Branch using `>=` instead of the more correct `==`
        // to tell the compiler that the pos..cap slice is always valid.
        if *this.pos >= *this.cap {
            debug_assert!(*this.pos == *this.cap);
            *this.cap = ready!(this.inner.as_mut().poll_read(cx, this.buffer))?;
            // Force the underlying reader to further go
            // through its states if needed.
            ready!(this.inner.poll_read(cx, &mut []))?;
            *this.pos = 0;
        }
        Poll::Ready(Ok(&this.buffer[*this.pos..*this.cap]))
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        *self.project().pos = self.cap.min(self.pos + amt);
    }
}
