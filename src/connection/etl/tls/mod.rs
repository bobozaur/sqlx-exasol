#[cfg(feature = "etl_native_tls")]
mod native_tls;
#[cfg(feature = "etl_rustls")]
mod rustls;
mod sync_socket;

use rcgen::{Certificate, CertificateParams, KeyPair, PKCS_RSA_SHA256};
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
use crate::error::ExaResultExt;

#[cfg(all(feature = "etl_native_tls", feature = "etl_rustls"))]
compile_error!("Only enable one of 'etl_antive_tls' or 'etl_rustls' features");

#[allow(unreachable_code)]
pub fn tls_with_socket_maker() -> Result<impl WithSocketMaker, SqlxError> {
    let cert = make_cert()?;

    #[cfg(feature = "etl_native_tls")]
    return NativeTlsSocketSpawner::new(&cert);
    #[cfg(feature = "etl_rustls")]
    return RustlsSocketSpawner::new(&cert);
}

fn make_cert() -> Result<Certificate, SqlxError> {
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

impl<T> ExaResultExt<T> for Result<T, rcgen::Error> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        self.map_err(|e| SqlxError::Tls(e.into()))
    }
}
