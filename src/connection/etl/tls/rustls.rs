use std::{
    future::poll_fn,
    io::{Error as IoError, ErrorKind as IoErrorKind, Read, Result as IoResult, Write},
    net::SocketAddrV4,
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures_core::future::BoxFuture;
use rcgen::{Certificate, KeyPair};
use rustls::{pki_types::PrivateKeyDer, ServerConfig, ServerConnection};
use sqlx_core::{
    error::Error as SqlxError,
    io::ReadBuf,
    net::{Socket, WithSocket},
};

use super::sync_socket::SyncSocket;
use crate::{
    connection::websocket::socket::{ExaSocket, WithExaSocket},
    error::ExaResultExt,
    etl::{get_etl_addr, traits::WithSocketMaker, SocketFuture, WithSocketFuture},
};

/// Implementor of [`WithSocketMaker`] used for the creation of [`WithRustlsSocket`].
pub struct RustlsSocketSpawner(Arc<ServerConfig>);

impl RustlsSocketSpawner {
    pub fn new(cert: &Certificate, key_pair: &KeyPair) -> Result<Self, SqlxError> {
        tracing::trace!("creating 'rustls' socket spawner");

        let tls_cert = cert.der().clone();
        let key = PrivateKeyDer::Pkcs8(key_pair.serialize_der().into());

        let config = {
            Arc::new(
                ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(vec![tls_cert], key)
                    .to_sqlx_err()?,
            )
        };

        Ok(Self(config))
    }
}

impl WithSocketMaker for RustlsSocketSpawner {
    type WithSocket = WithRustlsSocket;

    fn make_with_socket(&self, wrapper: WithExaSocket) -> Self::WithSocket {
        WithRustlsSocket::new(wrapper, self.0.clone())
    }
}

pub struct WithRustlsSocket {
    wrapper: WithExaSocket,
    config: Arc<ServerConfig>,
}

impl WithRustlsSocket {
    fn new(wrapper: WithExaSocket, config: Arc<ServerConfig>) -> Self {
        Self { wrapper, config }
    }

    fn map_tls_error(e: rustls::Error) -> IoError {
        IoError::new(IoErrorKind::Other, e)
    }

    async fn wrap_socket<S: Socket>(self, socket: S) -> IoResult<ExaSocket> {
        let state = ServerConnection::new(self.config).map_err(Self::map_tls_error)?;
        let mut socket = RustlsSocket {
            inner: SyncSocket(socket),
            state,
            close_notify_sent: false,
        };

        // Performs the TLS handshake or bails
        poll_fn(|cx| socket.poll_read_ready(cx)).await?;
        poll_fn(|cx| socket.poll_write_ready(cx)).await?;

        let socket = self.wrapper.with_socket(socket);
        Ok(socket)
    }

    async fn work<S: Socket>(self, socket: S) -> Result<(SocketAddrV4, SocketFuture), SqlxError> {
        let (socket, address) = get_etl_addr(socket).await?;
        let future: BoxFuture<'_, IoResult<ExaSocket>> = Box::pin(self.wrap_socket(socket));
        Ok((address, future))
    }
}

impl WithSocket for WithRustlsSocket {
    type Output = WithSocketFuture;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        Box::pin(self.work(socket))
    }
}

struct RustlsSocket<S>
where
    S: Socket,
{
    inner: SyncSocket<S>,
    state: ServerConnection,
    close_notify_sent: bool,
}

impl<S> Socket for RustlsSocket<S>
where
    S: Socket,
{
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> IoResult<usize> {
        self.state.reader().read(buf.init_mut())
    }

    fn try_write(&mut self, buf: &[u8]) -> IoResult<usize> {
        match self.state.writer().write(buf) {
            // Returns a zero-length write when the buffer is full.
            Ok(0) => Err(IoErrorKind::WouldBlock.into()),
            other => other,
        }
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        loop {
            match self.state.complete_io(&mut self.inner) {
                Err(e) if e.kind() == IoErrorKind::WouldBlock => (),
                ready => return Poll::Ready(ready.map(|_| ())),
            };

            ready!(self.inner.poll_read_ready(cx))?;
        }
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        loop {
            match self.state.complete_io(&mut self.inner) {
                Err(e) if e.kind() == IoErrorKind::WouldBlock => (),
                ready => return Poll::Ready(ready.map(|_| ())),
            };

            ready!(self.inner.poll_write_ready(cx))?;
        }
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.poll_write_ready(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        if !self.close_notify_sent {
            self.state.send_close_notify();
            self.close_notify_sent = true;
        }

        ready!(self.poll_read_ready(cx))?;
        ready!(self.poll_write_ready(cx))?;

        // Some socket implementations (like `tokio`) try to implicitly shutdown the TCP stream.
        // That results in a sporadic error because `rustls` already initiates the close.
        match ready!(self.inner.0.poll_shutdown(cx)) {
            Err(e) if e.kind() == IoErrorKind::NotConnected => Poll::Ready(Ok(())),
            res => Poll::Ready(res),
        }
    }
}

impl<T> ExaResultExt<T> for Result<T, rustls::Error> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        self.map_err(|e| SqlxError::Tls(e.into()))
    }
}
