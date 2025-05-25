use std::{
    future::poll_fn,
    io::{Error as IoError, ErrorKind as IoErrorKind, Read, Result as IoResult, Write},
    net::SocketAddrV4,
    sync::Arc,
    task::{ready, Context, Poll},
};

use futures_core::future::BoxFuture;
use native_tls::{HandshakeError, Identity, TlsAcceptor, TlsStream};
use rcgen::{Certificate, KeyPair};
use sqlx_core::{
    error::Error as SqlxError,
    io::ReadBuf,
    net::{Socket, WithSocket},
};

use super::sync_socket::SyncSocket;
use crate::{
    connection::websocket::socket::{ExaSocket, WithExaSocket},
    error::ToSqlxError,
    etl::{get_etl_addr, traits::WithSocketMaker, SocketFuture},
};

/// Implementor of [`WithSocketMaker`] used for the creation of [`WithNativeTlsSocket`].
pub struct NativeTlsSocketSpawner(Arc<TlsAcceptor>);

impl NativeTlsSocketSpawner {
    pub fn new(cert: &Certificate, key_pair: &KeyPair) -> Result<Self, SqlxError> {
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

impl WithSocketMaker for NativeTlsSocketSpawner {
    type WithSocket = WithNativeTlsSocket;

    fn make_with_socket(&self, wrapper: WithExaSocket) -> Self::WithSocket {
        WithNativeTlsSocket::new(wrapper, self.0.clone())
    }
}

pub struct WithNativeTlsSocket {
    wrapper: WithExaSocket,
    acceptor: Arc<TlsAcceptor>,
}

impl WithNativeTlsSocket {
    fn new(wrapper: WithExaSocket, acceptor: Arc<TlsAcceptor>) -> Self {
        Self { wrapper, acceptor }
    }

    fn map_tls_error(e: native_tls::Error) -> IoError {
        IoError::new(IoErrorKind::Other, e)
    }

    async fn wrap_socket<S: Socket>(self, socket: S) -> IoResult<ExaSocket> {
        let mut res = self.acceptor.accept(SyncSocket(socket));

        loop {
            match res {
                Ok(s) => return Ok(self.wrapper.with_socket(NativeTlsSocket(s)).await),
                Err(HandshakeError::Failure(e)) => return Err(Self::map_tls_error(e)),
                Err(HandshakeError::WouldBlock(mut h)) => {
                    poll_fn(|cx| h.get_mut().poll_read_ready(cx)).await?;
                    poll_fn(|cx| h.get_mut().poll_write_ready(cx)).await?;
                    res = h.handshake();
                }
            };
        }
    }

    async fn work<S: Socket>(self, socket: S) -> Result<(SocketAddrV4, SocketFuture), SqlxError> {
        let (socket, address) = get_etl_addr(socket).await?;
        let future: BoxFuture<'_, IoResult<ExaSocket>> = Box::pin(self.wrap_socket(socket));
        Ok((address, future))
    }
}

impl WithSocket for WithNativeTlsSocket {
    type Output = Result<(SocketAddrV4, SocketFuture), SqlxError>;

    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        self.work(socket).await
    }
}

struct NativeTlsSocket<S>(TlsStream<SyncSocket<S>>)
where
    S: Socket;

impl<S> Socket for NativeTlsSocket<S>
where
    S: Socket,
{
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> IoResult<usize> {
        self.0.read(buf.init_mut())
    }

    fn try_write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.0.write(buf)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.0.get_mut().poll_read_ready(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.0.get_mut().poll_write_ready(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.0.shutdown() {
            Err(e) if e.kind() == IoErrorKind::WouldBlock => (),
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
