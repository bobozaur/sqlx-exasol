use std::{
    future::poll_fn,
    io::{self, Read, Write},
    sync::Arc,
    task::{ready, Context, Poll},
};

use rcgen::{Certificate, KeyPair};
use rustls::{pki_types::PrivateKeyDer, ServerConfig, ServerConnection};
use sqlx_core::{
    io::ReadBuf,
    net::{Socket, WithSocket},
};

use super::sync_socket::SyncSocket;
use crate::{
    connection::websocket::socket::{ExaSocket, WithExaSocket},
    error::ToSqlxError,
    etl::{with_socket::WithSocketMaker, WithSocketFuture},
    SqlxError, SqlxResult,
};

/// Implementor of [`WithSocketMaker`] used for the creation of [`WithRustlsSocket`].
pub struct RustlsSocketSpawner(Arc<ServerConfig>);

impl RustlsSocketSpawner {
    pub fn new(cert: &Certificate, key_pair: &KeyPair) -> SqlxResult<Self> {
        tracing::trace!("creating 'rustls' socket spawner");

        let tls_cert = cert.der().clone();
        let key = PrivateKeyDer::Pkcs8(key_pair.serialize_der().into());

        let config = {
            Arc::new(
                ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(vec![tls_cert], key)
                    .map_err(ToSqlxError::to_sqlx_err)?,
            )
        };

        Ok(Self(config))
    }
}

impl WithSocketMaker for RustlsSocketSpawner {
    type WithSocket = WithRustlsSocket;

    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket {
        WithRustlsSocket::new(with_socket, self.0.clone())
    }
}

pub struct WithRustlsSocket {
    inner: WithExaSocket,
    config: Arc<ServerConfig>,
}

impl WithRustlsSocket {
    fn new(inner: WithExaSocket, config: Arc<ServerConfig>) -> Self {
        Self { inner, config }
    }

    async fn wrap_socket<S: Socket>(self, socket: S) -> io::Result<ExaSocket> {
        let state = ServerConnection::new(self.config)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let mut socket = RustlsSocket {
            inner: SyncSocket(socket),
            state,
            close_notify_sent: false,
        };

        // Performs the TLS handshake or bails
        poll_fn(|cx| socket.poll_read_ready(cx)).await?;
        poll_fn(|cx| socket.poll_write_ready(cx)).await?;

        let socket = self.inner.with_socket(socket).await;
        Ok(socket)
    }
}

impl WithSocket for WithRustlsSocket {
    type Output = SqlxResult<WithSocketFuture>;

    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        Ok(Box::pin(self.wrap_socket(socket)))
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
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> io::Result<usize> {
        self.state.reader().read(buf.init_mut())
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.state.writer().write(buf) {
            // Returns a zero-length write when the buffer is full.
            Ok(0) => Err(io::ErrorKind::WouldBlock.into()),
            other => other,
        }
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match self.state.complete_io(&mut self.inner) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                ready => return Poll::Ready(ready.map(|_| ())),
            };

            ready!(self.inner.poll_read_ready(cx))?;
        }
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match self.state.complete_io(&mut self.inner) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                ready => return Poll::Ready(ready.map(|_| ())),
            };

            ready!(self.inner.poll_write_ready(cx))?;
        }
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_write_ready(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.close_notify_sent {
            self.state.send_close_notify();
            self.close_notify_sent = true;
        }

        ready!(self.poll_read_ready(cx))?;
        ready!(self.poll_write_ready(cx))?;

        // Some socket implementations (like `tokio`) try to implicitly shutdown the TCP stream.
        // That results in a sporadic error because `rustls` already initiates the close.
        match ready!(self.inner.0.poll_shutdown(cx)) {
            Err(e) if e.kind() == io::ErrorKind::NotConnected => Poll::Ready(Ok(())),
            res => Poll::Ready(res),
        }
    }
}

impl ToSqlxError for rustls::Error {
    fn to_sqlx_err(self) -> SqlxError {
        SqlxError::Tls(self.into())
    }
}
