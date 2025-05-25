#[cfg(feature = "etl_native_tls")]
mod native_tls;
#[cfg(feature = "etl_rustls")]
mod rustls;
mod sync_socket;

use rcgen::{CertificateParams, KeyPair};
use rsa::{
    pkcs8::{EncodePrivateKey, LineEnding},
    RsaPrivateKey,
};
use sqlx_core::error::Error as SqlxError;

#[cfg(feature = "etl_native_tls")]
use self::native_tls::NativeTlsSocketSpawner;
#[cfg(feature = "etl_rustls")]
use self::rustls::RustlsSocketSpawner;
use super::traits::WithSocketMaker;
use crate::error::ToSqlxError;

#[cfg(all(feature = "etl_native_tls", feature = "etl_rustls"))]
compile_error!("Only enable one of 'etl_antive_tls' or 'etl_rustls' features");

#[allow(unreachable_code)]
pub fn tls_with_socket_maker() -> Result<impl WithSocketMaker, SqlxError> {
    let key_pair = make_key()?;
    let cert = CertificateParams::default()
        .self_signed(&key_pair)
        .map_err(ToSqlxError::to_sqlx_err)?;

    #[cfg(feature = "etl_native_tls")]
    return NativeTlsSocketSpawner::new(&cert, &key_pair);
    #[cfg(feature = "etl_rustls")]
    return RustlsSocketSpawner::new(&cert, &key_pair);
}

fn make_key() -> Result<KeyPair, SqlxError> {
    let mut rng = rand::thread_rng();
    let bits = 2048;
    let private_key = RsaPrivateKey::new(&mut rng, bits).map_err(ToSqlxError::to_sqlx_err)?;

    let key = private_key
        .to_pkcs8_pem(LineEnding::CRLF)
        .map_err(From::from)
        .map_err(SqlxError::Tls)?;

    KeyPair::from_pem(&key).map_err(ToSqlxError::to_sqlx_err)
}

impl ToSqlxError for rcgen::Error {
    fn to_sqlx_err(self) -> SqlxError {
        SqlxError::Tls(self.into())
    }
}
