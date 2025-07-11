use std::{
    future::poll_fn,
    io::{self, Read, Write},
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures_util::FutureExt;
use native_tls::{HandshakeError, Identity, TlsAcceptor, TlsStream};
use rcgen::{Certificate, KeyPair};
use sqlx_core::{
    io::ReadBuf,
    net::{Socket, WithSocket},
};

use crate::{
    connection::websocket::socket::{ExaSocket, WithExaSocket},
    error::ToSqlxError,
    etl::job::{maybe_tls::tls::sync_socket::SyncSocket, SocketSetup, WithSocketMaker},
    SqlxError, SqlxResult,
};

/// Implementor of [`WithSocketMaker`] used for the creation of [`WithNativeTlsSocket`].
pub struct WithNativeTlsSocketMaker(Arc<TlsAcceptor>);

impl WithNativeTlsSocketMaker {
    pub fn new(cert: &Certificate, key_pair: &KeyPair) -> SqlxResult<Self> {
        tracing::trace!("creating 'native-tls' socket spawner");

        let tls_cert = cert.pem();
        let key = key_pair.serialize_pem();

        let ident = Identity::from_pkcs8(tls_cert.as_bytes(), key.as_bytes())
            .map_err(ToSqlxError::to_sqlx_err)?;
        let acceptor = TlsAcceptor::new(ident).map_err(ToSqlxError::to_sqlx_err)?;
        let acceptor = Arc::new(acceptor);

        Ok(Self(acceptor))
    }
}

impl WithSocketMaker for WithNativeTlsSocketMaker {
    type WithSocket = WithNativeTlsSocket;

    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket {
        WithNativeTlsSocket::new(with_socket, self.0.clone())
    }
}

/// Implementor of [`WithSocket`] for [`native_tls`].
pub struct WithNativeTlsSocket {
    inner: WithExaSocket,
    acceptor: Arc<TlsAcceptor>,
}

impl WithNativeTlsSocket {
    fn new(inner: WithExaSocket, acceptor: Arc<TlsAcceptor>) -> Self {
        Self { inner, acceptor }
    }

    async fn wrap_socket<S: Socket>(self, socket: S) -> io::Result<ExaSocket> {
        let mut res = self.acceptor.accept(SyncSocket(socket));

        loop {
            match res {
                Ok(s) => return Ok(self.inner.with_socket(NativeTlsSocket(s)).await),
                Err(HandshakeError::Failure(e)) => {
                    return Err(io::Error::new(io::ErrorKind::Other, e))
                }
                Err(HandshakeError::WouldBlock(mut h)) => {
                    poll_fn(|cx| h.get_mut().poll_read_ready(cx)).await?;
                    poll_fn(|cx| h.get_mut().poll_write_ready(cx)).await?;
                    res = h.handshake();
                }
            };
        }
    }
}

impl WithSocket for WithNativeTlsSocket {
    type Output = SocketSetup;

    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        self.wrap_socket(socket).boxed()
    }
}

struct NativeTlsSocket<S>(TlsStream<SyncSocket<S>>)
where
    S: Socket;

impl<S> Socket for NativeTlsSocket<S>
where
    S: Socket,
{
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> io::Result<usize> {
        self.0.read(buf.init_mut())
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.get_mut().poll_read_ready(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.get_mut().poll_write_ready(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.0.shutdown() {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
            ready => return Poll::Ready(ready),
        };

        ready!(self.0.get_mut().poll_read_ready(cx))?;
        self.0.get_mut().poll_write_ready(cx)
    }
}

impl ToSqlxError for native_tls::Error {
    fn to_sqlx_err(self) -> SqlxError {
        SqlxError::Tls(self.into())
    }
}

impl<S> ToSqlxError for HandshakeError<S> {
    fn to_sqlx_err(self) -> SqlxError {
        SqlxError::Tls("native_tls handshake error".into())
    }
}
