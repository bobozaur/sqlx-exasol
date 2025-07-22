#[cfg(feature = "native-tls")]
mod native_tls;
#[cfg(feature = "rustls")]
mod rustls;
mod sync_socket;

use rcgen::{CertificateParams, KeyPair};
use rsa::{
    pkcs8::{EncodePrivateKey, LineEnding},
    RsaPrivateKey,
};

use crate::{error::ToSqlxError, SqlxError, SqlxResult};

#[cfg(feature = "native-tls")]
pub type WithTlsSocketMaker = native_tls::WithNativeTlsSocketMaker;
#[cfg(feature = "rustls")]
pub type WithTlsSocketMaker = rustls::WithRustlsSocketMaker;

#[cfg(feature = "native-tls")]
pub type WithTlsSocket = native_tls::WithNativeTlsSocket;
#[cfg(feature = "rustls")]
pub type WithTlsSocket = rustls::WithRustlsSocket;

/// Returns the dedicated [`impl WithSocketMaker`] for the chosen TLS implementation.
/// constructed.
pub fn with_worker() -> SqlxResult<WithTlsSocketMaker> {
    let bits = 2048;
    let private_key =
        RsaPrivateKey::new(&mut rand::thread_rng(), bits).map_err(ToSqlxError::to_sqlx_err)?;

    let key = private_key
        .to_pkcs8_pem(LineEnding::CRLF)
        .map_err(From::from)
        .map_err(SqlxError::Tls)?;

    let key_pair = KeyPair::from_pem(&key).map_err(ToSqlxError::to_sqlx_err)?;
    let cert = CertificateParams::default()
        .self_signed(&key_pair)
        .map_err(ToSqlxError::to_sqlx_err)?;

    #[cfg(feature = "native-tls")]
    return native_tls::WithNativeTlsSocketMaker::new(&cert, &key_pair);
    #[cfg(feature = "rustls")]
    return rustls::WithRustlsSocketMaker::new(&cert, &key_pair);
}

impl ToSqlxError for rcgen::Error {
    fn to_sqlx_err(self) -> SqlxError {
        SqlxError::Tls(self.into())
    }
}
