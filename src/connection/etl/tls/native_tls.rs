use arrayvec::ArrayString;
use futures_core::future::BoxFuture;
use native_tls::{HandshakeError, Identity, TlsAcceptor};
use rcgen::Certificate;
use sqlx_core::error::Error as SqlxError;
use sqlx_core::io::ReadBuf;
use sqlx_core::net::Socket;
use std::fmt::Write as _;
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Read, Result as IoResult, Write};
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::connection::websocket::socket::ExaSocket;
use crate::connection::websocket::socket::WithExaSocket;
use crate::error::ExaResultExt;
use crate::etl::get_etl_addr;
use sqlx_core::net::WithSocket;

use super::sync_socket::SyncSocket;

pub async fn native_tls_socket_spawners(
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
    tracing::trace!("spawning {num_sockets} TLS sockets through 'native-tls'");

    let tls_cert = cert.serialize_pem().to_sqlx_err()?;
    let key = cert.serialize_private_key_pem();

    let ident = Identity::from_pkcs8(tls_cert.as_bytes(), key.as_bytes()).to_sqlx_err()?;
    let acceptor = TlsAcceptor::new(ident).to_sqlx_err()?;
    let acceptor = Arc::new(acceptor);

    let mut output = Vec::with_capacity(ips.len());

    for ip in ips.into_iter().take(num_sockets) {
        let mut ip_buf = ArrayString::<50>::new_const();
        write!(&mut ip_buf, "{ip}").expect("IP address should fit in 50 characters");

        let wrapper = WithExaSocket(SocketAddr::new(ip, port));
        let with_socket = WithNativeTlsSocket::new(wrapper, acceptor.clone());
        let future = sqlx_core::net::connect_tcp(&ip_buf, port, with_socket).await?;

        output.push(future);
    }

    Ok(output)
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

struct WithNativeTlsSocket {
    wrapper: WithExaSocket,
    acceptor: Arc<TlsAcceptor>,
}

impl WithNativeTlsSocket {
    fn new(wrapper: WithExaSocket, acceptor: Arc<TlsAcceptor>) -> Self {
        Self { wrapper, acceptor }
    }
}

impl WithSocket for WithNativeTlsSocket {
    type Output = BoxFuture<
        'static,
        Result<(SocketAddrV4, BoxFuture<'static, IoResult<ExaSocket>>), SqlxError>,
    >;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        let WithNativeTlsSocket { wrapper, acceptor } = self;

        Box::pin(async move {
            let (socket, address) = get_etl_addr(socket).await?;

            let future: BoxFuture<IoResult<ExaSocket>> = Box::pin(async move {
                let mut hs = match acceptor.accept(SyncSocket::new(socket)) {
                    Ok(s) => return Ok(wrapper.with_socket(NativeTlsSocket(s))),
                    Err(HandshakeError::Failure(e)) => {
                        return Err(IoError::new(IoErrorKind::Other, e))
                    }
                    Err(HandshakeError::WouldBlock(hs)) => hs,
                };

                loop {
                    hs.get_mut().ready().await?;

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
