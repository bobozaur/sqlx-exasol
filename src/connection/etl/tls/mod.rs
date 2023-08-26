#[cfg(feature = "etl_native_tls")]
mod native_tls;
#[cfg(feature = "etl_rustls")]
mod rustls;

use std::future;
use std::io::{Read, Result as IoResult, Write};
use std::net::{IpAddr, SocketAddrV4};
use std::task::{ready, Context, Poll};

use futures_channel::oneshot::Receiver;
use futures_core::future::BoxFuture;
use sqlx_core::error::Error as SqlxError;
use sqlx_core::net::Socket;

use crate::connection::websocket::socket::ExaSocket;

use rcgen::{Certificate, CertificateParams, KeyPair, PKCS_RSA_SHA256};
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use rsa::RsaPrivateKey;

use crate::error::ExaResultExt;

// #[cfg(all(feature = "etl_native_tls", feature = "etl_rustls"))]
// compile_error!("Only enable one of 'etl_antive_tls' or 'etl_rustls' features");

#[allow(unreachable_code)]
pub async fn spawn_tls_sockets(
    num_sockets: usize,
    ips: Vec<IpAddr>,
    port: u16,
) -> Result<
    Vec<(
        BoxFuture<'static, Result<ExaSocket, SqlxError>>,
        Receiver<SocketAddrV4>,
    )>,
    SqlxError,
> {
    let cert = make_cert()?;

    #[cfg(feature = "etl_native_tls")]
    return native_tls::spawn_native_tls_sockets(num_sockets, ips, port, cert).await;
    #[cfg(feature = "etl_rustls")]
    return rustls::spawn_rustls_sockets(num_sockets, ips, port, cert).await;
}

pub fn make_cert() -> Result<Certificate, SqlxError> {
    let mut rng = rand::thread_rng();
    let bits = 2048;
    let private_key = RsaPrivateKey::new(&mut rng, bits).to_sqlx_err()?;

    let key = private_key
        .to_pkcs8_pem(LineEnding::CRLF)
        .map_err(From::from)
        .map_err(SqlxError::Tls)?;

    let key_pair = KeyPair::from_pem(&key).to_sqlx_err()?;

    let mut params = CertificateParams::default();
    params.alg = &PKCS_RSA_SHA256;
    params.key_pair = Some(key_pair);

    Certificate::from_params(params).to_sqlx_err()
}

pub struct SyncSocket<S>
where
    S: Socket,
{
    socket: S,
    wants_read: bool,
    wants_write: bool,
}

impl<S> SyncSocket<S>
where
    S: Socket,
{
    pub fn new(socket: S) -> Self {
        Self {
            socket,
            wants_read: false,
            wants_write: false,
        }
    }
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        if self.wants_write {
            ready!(self.socket.poll_write_ready(cx))?;
            self.wants_write = false;
        }

        if self.wants_read {
            ready!(self.socket.poll_read_ready(cx))?;
            self.wants_read = false;
        }

        Poll::Ready(Ok(()))
    }

    #[allow(dead_code)]
    pub async fn ready(&mut self) -> IoResult<()> {
        future::poll_fn(|cx| self.poll_ready(cx)).await
    }
}

impl<S> Read for SyncSocket<S>
where
    S: Socket,
{
    fn read(&mut self, mut buf: &mut [u8]) -> IoResult<usize> {
        self.wants_read = true;
        let read = self.socket.try_read(&mut buf)?;
        self.wants_read = false;

        Ok(read)
    }
}

impl<S> Write for SyncSocket<S>
where
    S: Socket,
{
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.wants_write = true;
        let written = self.socket.try_write(buf)?;
        self.wants_write = false;
        Ok(written)
    }

    fn flush(&mut self) -> IoResult<()> {
        // NOTE: TCP sockets and unix sockets are both no-ops for flushes
        Ok(())
    }
}
