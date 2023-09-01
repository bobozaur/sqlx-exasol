#[cfg(feature = "etl_native_tls")]
mod native_tls;
#[cfg(feature = "etl_rustls")]
mod rustls;
#[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
mod sync_socket;

use std::io::Result as IoResult;
use std::net::{IpAddr, SocketAddrV4};

use futures_core::future::BoxFuture;
use sqlx_core::error::Error as SqlxError;

use crate::connection::websocket::socket::ExaSocket;

use rcgen::{Certificate, CertificateParams, KeyPair, PKCS_RSA_SHA256};
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use rsa::RsaPrivateKey;

use crate::error::ExaResultExt;

// #[cfg(all(feature = "etl_native_tls", feature = "etl_rustls"))]
// compile_error!("Only enable one of 'etl_antive_tls' or 'etl_rustls' features");

#[allow(unreachable_code)]
pub async fn tls_socket_spawners(
    num_sockets: usize,
    ips: Vec<IpAddr>,
    port: u16,
) -> Result<
    Vec<
        BoxFuture<
            'static,
            Result<(SocketAddrV4, BoxFuture<'static, IoResult<ExaSocket>>), SqlxError>,
        >,
    >,
    SqlxError,
> {
    let cert = make_cert()?;

    #[cfg(feature = "etl_native_tls")]
    return native_tls::native_tls_socket_spawners(num_sockets, ips, port, cert).await;
    #[cfg(feature = "etl_rustls")]
    return rustls::rustls_socket_spawners(num_sockets, ips, port, cert).await;
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
