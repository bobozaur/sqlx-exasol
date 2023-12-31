use std::{
    future,
    io::{Error as IoError, ErrorKind as IoErrorKind, Read, Result as IoResult, Write},
    net::SocketAddrV4,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures_core::future::BoxFuture;
use rcgen::Certificate;
use rustls::{Certificate as RustlsCert, PrivateKey, ServerConfig, ServerConnection};
use sqlx_core::{
    error::Error as SqlxError,
    io::ReadBuf,
    net::{Socket, WithSocket},
};

use super::sync_socket::SyncSocket;
use crate::{
    connection::websocket::socket::{ExaSocket, WithExaSocket},
    error::ExaResultExt,
    etl::{get_etl_addr, traits::WithSocketMaker, SocketFuture},
};

/// Implementor of [`WithSocketMaker`] used for the creation of [`WithRustlsSocket`].
pub struct RustlsSocketSpawner(Arc<ServerConfig>);

impl RustlsSocketSpawner {
    pub fn new(cert: &Certificate) -> Result<Self, SqlxError> {
        tracing::trace!("creating 'rustls' socket spawner");

        let tls_cert = RustlsCert(cert.serialize_der().to_sqlx_err()?);
        let key = PrivateKey(cert.serialize_private_key_der());

        let config = {
            Arc::new(
                ServerConfig::builder()
                    .with_safe_defaults()
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
}

impl WithSocket for WithRustlsSocket {
    type Output = BoxFuture<'static, Result<(SocketAddrV4, SocketFuture), SqlxError>>;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        let WithRustlsSocket { wrapper, config } = self;

        Box::pin(async move {
            let (socket, address) = get_etl_addr(socket).await?;

            let future: BoxFuture<'_, IoResult<ExaSocket>> = Box::pin(async move {
                let state = ServerConnection::new(config)
                    .map_err(|e| IoError::new(IoErrorKind::Other, e))?;
                let mut socket = RustlsSocket {
                    inner: SyncSocket::new(socket),
                    state,
                    close_notify_sent: false,
                };

                // Performs the TLS handshake or bails
                socket.complete_io().await?;

                let socket = wrapper.with_socket(socket);
                Ok(socket)
            });

            Ok((address, future))
        })
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

impl<S> RustlsSocket<S>
where
    S: Socket,
{
    fn poll_complete_io(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        loop {
            match self.state.complete_io(&mut self.inner) {
                Err(e) if e.kind() == IoErrorKind::WouldBlock => {
                    ready!(self.inner.poll_ready(cx))?;
                }
                ready => return Poll::Ready(ready.map(|_| ())),
            }
        }
    }

    async fn complete_io(&mut self) -> IoResult<()> {
        future::poll_fn(|cx| self.poll_complete_io(cx)).await
    }
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
        self.poll_complete_io(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.poll_complete_io(cx)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.poll_complete_io(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        if !self.close_notify_sent {
            self.state.send_close_notify();
            self.close_notify_sent = true;
        }

        ready!(self.poll_complete_io(cx))?;
        Pin::new(&mut self.inner.socket).poll_shutdown(cx)
    }
}

impl<T> ExaResultExt<T> for Result<T, rustls::Error> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        self.map_err(|e| SqlxError::Tls(e.into()))
    }
}
