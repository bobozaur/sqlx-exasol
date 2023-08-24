use std::io::{ErrorKind as IoErrorKind, Read, Result as IoResult, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_io::AsyncWrite;
use rcgen::Certificate;
use rustls::PrivateKey;
use rustls::ServerConfig;
use rustls::{Certificate as RustlsCert, ServerConnection};
use sqlx_core::error::Error as SqlxError;
use sqlx_core::io::ReadBuf;
use sqlx_core::net::{Socket, WithSocket};

use crate::connection::websocket::socket::ExaSocket;
use crate::connection::websocket::socket::WithExaSocket;
use crate::error::ExaResultExt;

use super::SyncExaSocket;

pub fn upgrade_rustls(socket: ExaSocket, cert: &Certificate) -> Result<ExaSocket, SqlxError> {
    let tls_cert = RustlsCert(cert.serialize_der().to_sqlx_err()?);
    let key = PrivateKey(cert.serialize_private_key_der());
    let socket_addr = socket.sock_addr;

    let config = {
        Arc::new(
            ServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth()
                .with_single_cert(vec![tls_cert], key)
                .to_sqlx_err()?,
        )
    };
    let state = ServerConnection::new(config).to_sqlx_err()?;
    let socket = RustlsSocket {
        inner: SyncExaSocket::new(socket),
        state,
        close_notify_sent: false,
    };

    let socket = WithExaSocket(socket_addr).with_socket(socket);
    Ok(socket)
}

struct RustlsSocket {
    inner: SyncExaSocket,
    state: ServerConnection,
    close_notify_sent: bool,
}

impl RustlsSocket {
    fn poll_complete_io(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        loop {
            match self.state.complete_io(&mut self.inner) {
                Err(e) if e.kind() == IoErrorKind::WouldBlock => {
                    futures_util::ready!(self.inner.poll_ready(cx))?;
                }
                ready => return Poll::Ready(ready.map(|_| ())),
            }
        }
    }
}

impl Socket for RustlsSocket {
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

        futures_util::ready!(self.poll_complete_io(cx))?;
        Pin::new(&mut self.inner.socket).poll_close(cx)
    }
}
