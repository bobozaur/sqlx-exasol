#[cfg(feature = "etl_native_tls")]
mod native_tls;
#[cfg(feature = "etl_rustls")]
mod rustls;

use std::io::{Read, Result as IoResult, Write};
use std::task::{ready, Context, Poll};

use sqlx_core::error::Error as SqlxError;

use crate::connection::websocket::socket::ExaSocket;

use rcgen::{Certificate, CertificateParams, KeyPair, PKCS_RSA_SHA256};
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use rsa::RsaPrivateKey;

use crate::error::ExaResultExt;

use self::native_tls::upgrade_native_tls;
use self::rustls::upgrade_rustls;

#[allow(unreachable_code)]
pub async fn upgrade(socket: ExaSocket, cert: &Certificate) -> Result<ExaSocket, SqlxError> {
    #[cfg(feature = "etl_native_tls")]
    return upgrade_native_tls(socket, cert);
    #[cfg(feature = "etl_rustls")]
    return upgrade_rustls(socket, cert);
}

pub fn make_cert() -> Result<Certificate, SqlxError> {
    let mut params = CertificateParams::default();
    params.alg = &PKCS_RSA_SHA256;
    params.key_pair = Some(make_rsa_keypair()?);
    Certificate::from_params(params).to_sqlx_err()
}

fn make_rsa_keypair() -> Result<KeyPair, SqlxError> {
    let mut rng = rand::thread_rng();
    let bits = 2048;
    let private_key = RsaPrivateKey::new(&mut rng, bits).to_sqlx_err()?;

    let key = private_key
        .to_pkcs8_pem(LineEnding::CRLF)
        .map_err(|e| SqlxError::Protocol(e.to_string()))?;

    KeyPair::from_pem(&key).to_sqlx_err()
}

pub struct SyncExaSocket {
    pub socket: ExaSocket,
    wants_read: bool,
    wants_write: bool,
}

impl SyncExaSocket {
    pub fn new(socket: ExaSocket) -> Self {
        Self {
            socket,
            wants_read: false,
            wants_write: false,
        }
    }
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        if self.wants_write {
            ready!(self.socket.inner.poll_write_ready(cx))?;
            self.wants_write = false;
        }

        if self.wants_read {
            ready!(self.socket.inner.poll_read_ready(cx))?;
            self.wants_read = false;
        }

        Poll::Ready(Ok(()))
    }
}

impl Read for SyncExaSocket {
    fn read(&mut self, mut buf: &mut [u8]) -> IoResult<usize> {
        self.wants_read = true;
        let read = self.socket.inner.try_read(&mut buf)?;
        self.wants_read = false;

        Ok(read)
    }
}

impl Write for SyncExaSocket {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.wants_write = true;
        let written = self.socket.inner.try_write(buf)?;
        self.wants_write = false;
        Ok(written)
    }

    fn flush(&mut self) -> IoResult<()> {
        // NOTE: TCP sockets and unix sockets are both no-ops for flushes
        Ok(())
    }
}
