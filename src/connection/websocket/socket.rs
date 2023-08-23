use std::{
    fmt::Debug,
    io::{ErrorKind as IoErrorKind, Result as IoResult},
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::ready;
use futures_io::{AsyncRead, AsyncWrite};
use sqlx_core::{
    bytes::BufMut,
    net::{Socket, WithSocket},
};

/// Implementor of [`WithSocket`].
pub struct WithExaSocket(pub SocketAddr);

impl WithSocket for WithExaSocket {
    type Output = ExaSocket;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        ExaSocket {
            sock_addr: self.0,
            inner: Box::new(socket),
        }
    }
}

/// A wrapper so we can implement [`AsyncRead`] and [`AsyncWrite`]
/// for the underlying TCP socket. The traits are needed by the
/// [`WebSocketStream`] wrapper.
pub struct ExaSocket {
    pub sock_addr: SocketAddr,
    pub inner: Box<dyn Socket>,
}

impl Debug for ExaSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", stringify!(RwSocket))
    }
}

impl AsyncRead for ExaSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: &mut [u8],
    ) -> Poll<IoResult<usize>> {
        while buf.has_remaining_mut() {
            match self.inner.try_read(&mut buf) {
                Err(e) if e.kind() == IoErrorKind::WouldBlock => {
                    ready!(self.inner.poll_read_ready(cx)?);
                }
                ready => return Poll::Ready(ready),
            }
        }

        Poll::Ready(Ok(0))
    }
}

impl AsyncWrite for ExaSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        while !buf.is_empty() {
            match self.inner.try_write(buf) {
                Err(e) if e.kind() == IoErrorKind::WouldBlock => {
                    ready!(self.inner.poll_write_ready(cx)?)
                }
                ready => return Poll::Ready(ready),
            }
        }

        Poll::Ready(Ok(0))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.inner.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.inner.poll_shutdown(cx)
    }
}
