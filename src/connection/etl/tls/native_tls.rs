use native_tls::{HandshakeError, Identity, TlsAcceptor};
use rcgen::Certificate;
use sqlx_core::error::Error as SqlxError;
use sqlx_core::io::ReadBuf;
use sqlx_core::net::Socket;
use std::io::{ErrorKind as IoErrorKind, Read, Result as IoResult, Write};
use std::task::{Context, Poll};

use crate::connection::websocket::socket::ExaSocket;
use crate::connection::websocket::socket::WithExaSocket;
use crate::error::ExaResultExt;
use sqlx_core::net::WithSocket;

use super::SyncExaSocket;

pub async fn upgrade_native_tls(
    socket: ExaSocket,
    cert: &Certificate,
) -> Result<ExaSocket, SqlxError> {
    tracing::trace!("upgrading socket to TLS through 'native-tls'");

    let tls_cert = cert.serialize_pem().to_sqlx_err()?;
    let key = cert.serialize_private_key_pem();
    let socket_addr = socket.sock_addr;

    let ident = Identity::from_pkcs8(tls_cert.as_bytes(), key.as_bytes()).to_sqlx_err()?;
    let connector = TlsAcceptor::new(ident).to_sqlx_err()?;

    let mut hs = match connector.accept(SyncExaSocket::new(socket)) {
        Ok(s) => return Ok(WithExaSocket(socket_addr).with_socket(NativeTlsSocket(s))),
        Err(HandshakeError::Failure(e)) => return Err(SqlxError::Tls(e.into())),
        Err(HandshakeError::WouldBlock(hs)) => hs,
    };

    loop {
        hs.get_mut().ready().await?;

        match hs.handshake() {
            Ok(s) => return Ok(WithExaSocket(socket_addr).with_socket(NativeTlsSocket(s))),
            Err(HandshakeError::Failure(e)) => return Err(SqlxError::Tls(e.into())),
            Err(HandshakeError::WouldBlock(h)) => hs = h,
        }
    }
}

struct NativeTlsSocket(native_tls::TlsStream<SyncExaSocket>);

impl Socket for NativeTlsSocket {
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
