use std::{
    future,
    io::{Error as IoError, ErrorKind as IoErrorKind, Read, Result as IoResult, Write},
    net::SocketAddrV4,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::future::BoxFuture;
use native_tls::{HandshakeError, Identity, TlsAcceptor};
use rcgen::Certificate;
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

/// Implementor of [`WithSocketMaker`] used for the creation of [`WithNativeTlsSocket`].
pub struct NativeTlsSocketSpawner(Arc<TlsAcceptor>);

impl NativeTlsSocketSpawner {
    pub fn new(cert: &Certificate) -> Result<Self, SqlxError> {
        tracing::trace!("creating 'native-tls' socket spawner");

        let tls_cert = cert.serialize_pem().to_sqlx_err()?;
        let key = cert.serialize_private_key_pem();

        let ident = Identity::from_pkcs8(tls_cert.as_bytes(), key.as_bytes()).to_sqlx_err()?;
        let acceptor = TlsAcceptor::new(ident).to_sqlx_err()?;
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
}

impl WithSocket for WithNativeTlsSocket {
    type Output = BoxFuture<'static, Result<(SocketAddrV4, SocketFuture), SqlxError>>;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        let WithNativeTlsSocket { wrapper, acceptor } = self;

        Box::pin(async move {
            let (socket, address) = get_etl_addr(socket).await?;

            let future: BoxFuture<'_, IoResult<ExaSocket>> = Box::pin(async move {
                let mut hs = match acceptor.accept(SyncSocket::new(socket)) {
                    Ok(s) => return Ok(wrapper.with_socket(NativeTlsSocket(s))),
                    Err(HandshakeError::Failure(e)) => {
                        return Err(IoError::new(IoErrorKind::Other, e))
                    }
                    Err(HandshakeError::WouldBlock(hs)) => hs,
                };

                loop {
                    future::poll_fn(|cx| hs.get_mut().poll_ready(cx)).await?;

                    match hs.handshake() {
                        Ok(s) => return Ok(wrapper.with_socket(NativeTlsSocket(s))),
                        Err(HandshakeError::Failure(e)) => {
                            return Err(IoError::new(IoErrorKind::Other, e))
                        }
                        Err(HandshakeError::WouldBlock(h)) => hs = h,
                    }
                }
            });

            Ok((address, future))
        })
    }
}

struct NativeTlsSocket<S>(native_tls::TlsStream<SyncSocket<S>>)
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
        self.0.get_mut().poll_ready(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        self.0.get_mut().poll_ready(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.0.shutdown() {
            Err(e) if e.kind() == IoErrorKind::WouldBlock => self.0.get_mut().poll_ready(cx),
            ready => Poll::Ready(ready),
        }
    }
}

impl<T> ExaResultExt<T> for Result<T, native_tls::Error> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        self.map_err(|e| SqlxError::Tls(e.into()))
    }
}

impl<T, S> ExaResultExt<T> for Result<T, HandshakeError<S>> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        self.map_err(|_| SqlxError::Tls("native_tls handshake error".into()))
    }
}
