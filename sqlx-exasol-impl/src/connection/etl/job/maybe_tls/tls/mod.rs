#[cfg(feature = "native-tls")]
mod native_tls;
#[cfg(feature = "rustls")]
mod rustls;
mod sync_socket;

use std::io;

use hex::ToHex;
use rcgen::{CertificateParams, KeyPair};
use rsa::{
    pkcs8::{EncodePrivateKey, LineEnding},
    RsaPrivateKey,
};
use sqlx_core::net::{Socket, WithSocket};

use crate::{
    connection::websocket::socket::{ExaSocket, WithExaSocket},
    error::ToSqlxError,
    etl::job::WithSocketMaker,
    SqlxError, SqlxResult,
};

pub enum WithTlsSocketMaker {
    #[cfg(feature = "rustls")]
    Rustls(rustls::WithRustlsSocketMaker),
    #[cfg(feature = "native-tls")]
    NativeTls(native_tls::WithNativeTlsSocketMaker),
}

impl WithTlsSocketMaker {
    pub fn new(with_pub_key: bool) -> SqlxResult<(WithTlsSocketMaker, Option<String>)> {
        let bits = 2048;
        let private_key =
            RsaPrivateKey::new(&mut rand::thread_rng(), bits).map_err(ToSqlxError::to_sqlx_err)?;

        let key = private_key
            .to_pkcs8_pem(LineEnding::CRLF)
            .map_err(From::from)
            .map_err(SqlxError::Tls)?;

        let key_pair = KeyPair::from_pem(&key).map_err(ToSqlxError::to_sqlx_err)?;
        let public_key = with_pub_key.then(|| key_pair.public_key_der().encode_hex_upper());
        let cert = CertificateParams::default()
            .self_signed(&key_pair)
            .map_err(ToSqlxError::to_sqlx_err)?;

        #[cfg(feature = "rustls")]
        return rustls::WithRustlsSocketMaker::new(&cert, &key_pair)
            .map(|wsm| (Self::Rustls(wsm), public_key));

        #[cfg(feature = "native-tls")]
        #[allow(unreachable_code, reason = "conditionally compiled")]
        return native_tls::WithNativeTlsSocketMaker::new(&cert, &key_pair)
            .map(|wsm| (Self::NativeTls(wsm), public_key));
    }
}

impl WithSocketMaker for WithTlsSocketMaker {
    type WithSocket = WithTlsSocket;

    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket {
        match self {
            #[cfg(feature = "rustls")]
            WithTlsSocketMaker::Rustls(m) => WithTlsSocket::Rustls(m.make_with_socket(with_socket)),
            #[cfg(feature = "native-tls")]
            WithTlsSocketMaker::NativeTls(m) => {
                WithTlsSocket::NativeTls(m.make_with_socket(with_socket))
            }
        }
    }
}

pub enum WithTlsSocket {
    #[cfg(feature = "rustls")]
    Rustls(rustls::WithRustlsSocket),
    #[cfg(feature = "native-tls")]
    NativeTls(native_tls::WithNativeTlsSocket),
}

impl WithSocket for WithTlsSocket {
    type Output = io::Result<ExaSocket>;

    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        match self {
            #[cfg(feature = "rustls")]
            WithTlsSocket::Rustls(w) => w.with_socket(socket).await,
            #[cfg(feature = "native-tls")]
            WithTlsSocket::NativeTls(w) => w.with_socket(socket).await,
        }
    }
}

impl ToSqlxError for rcgen::Error {
    fn to_sqlx_err(self) -> SqlxError {
        SqlxError::Tls(self.into())
    }
}
