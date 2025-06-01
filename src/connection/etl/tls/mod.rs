#[cfg(feature = "etl_native_tls")]
mod native_tls;
#[cfg(feature = "etl_rustls")]
mod rustls;
#[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
mod sync_socket;

use crate::{etl::with_socket::WithSocketMaker, SqlxError, SqlxResult};

#[cfg(not(any(feature = "etl_native_tls", feature = "etl_rustls")))]
pub fn with_socket_maker() -> SqlxResult<impl WithSocketMaker> {
    let err = SqlxError::Tls("No ETL TLS feature set".into());
    Err::<super::non_tls::NonTlsSocketSpawner, _>(err)
}

#[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
pub fn with_socket_maker() -> SqlxResult<impl WithSocketMaker> {
    use rcgen::{CertificateParams, KeyPair};
    use rsa::{
        pkcs8::{EncodePrivateKey, LineEnding},
        RsaPrivateKey,
    };

    use crate::error::ToSqlxError;

    impl ToSqlxError for rcgen::Error {
        fn to_sqlx_err(self) -> SqlxError {
            SqlxError::Tls(self.into())
        }
    }

    let mut rng = rand::thread_rng();
    let bits = 2048;
    let private_key = RsaPrivateKey::new(&mut rng, bits).map_err(ToSqlxError::to_sqlx_err)?;

    let key = private_key
        .to_pkcs8_pem(LineEnding::CRLF)
        .map_err(From::from)
        .map_err(SqlxError::Tls)?;

    let key_pair = KeyPair::from_pem(&key).map_err(ToSqlxError::to_sqlx_err)?;
    let cert = CertificateParams::default()
        .self_signed(&key_pair)
        .map_err(ToSqlxError::to_sqlx_err)?;

    #[cfg(feature = "etl_native_tls")]
    return native_tls::NativeTlsSocketSpawner::new(&cert, &key_pair);
    #[cfg(feature = "etl_rustls")]
    return rustls::RustlsSocketSpawner::new(&cert, &key_pair);
}

#[cfg(all(feature = "etl_native_tls", feature = "etl_rustls"))]
compile_error!("Only enable one of 'etl_antive_tls' or 'etl_rustls' features");
