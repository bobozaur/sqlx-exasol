//! This module provides the `hyper` I/O trait implementations for `ExaSocket`.
use std::{
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures_io::{AsyncRead, AsyncWrite};
use hyper::rt;

use crate::connection::websocket::socket::ExaSocket;

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
