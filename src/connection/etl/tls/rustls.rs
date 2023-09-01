use std::fmt::Write as _;
use std::future;
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Read, Result as IoResult, Write};
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use arrayvec::ArrayString;
use futures_core::future::BoxFuture;
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
use crate::etl::get_etl_addr;

use super::sync_socket::SyncSocket;

pub async fn rustls_socket_spawners(
    num_sockets: usize,
    ips: Vec<IpAddr>,
    port: u16,
    cert: Certificate,
) -> Result<
    Vec<
        BoxFuture<
            'static,
            Result<(SocketAddrV4, BoxFuture<'static, IoResult<ExaSocket>>), SqlxError>,
        >,
    >,
    SqlxError,
> {
    tracing::trace!("spawning {num_sockets} TLS sockets through 'rustls'");

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

    let mut output = Vec::with_capacity(ips.len());

    for ip in ips.into_iter().take(num_sockets) {
        let mut ip_buf = ArrayString::<50>::new_const();
        write!(&mut ip_buf, "{ip}").expect("IP address should fit in 50 characters");

        let wrapper = WithExaSocket(SocketAddr::new(ip, port));
        let with_socket = WithRustlsSocket::new(wrapper, config.clone());
        let future = sqlx_core::net::connect_tcp(&ip_buf, port, with_socket).await?;

        output.push(future);
    }

    Ok(output)
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

struct WithRustlsSocket {
    wrapper: WithExaSocket,
    config: Arc<ServerConfig>,
}

impl WithRustlsSocket {
    fn new(wrapper: WithExaSocket, config: Arc<ServerConfig>) -> Self {
        Self { wrapper, config }
    }
}

impl WithSocket for WithRustlsSocket {
    type Output = BoxFuture<
        'static,
        Result<(SocketAddrV4, BoxFuture<'static, IoResult<ExaSocket>>), SqlxError>,
    >;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        let WithRustlsSocket { wrapper, config } = self;

        Box::pin(async move {
            let (socket, address) = get_etl_addr(socket).await?;

            let future: BoxFuture<IoResult<ExaSocket>> = Box::pin(async move {
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
