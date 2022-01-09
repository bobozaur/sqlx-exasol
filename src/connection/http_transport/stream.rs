use super::TransportResult;
#[cfg(feature = "native-tls-basic")]
use crate::error::HttpTransportError;
#[cfg(feature = "native-tls-basic")]
use __native_tls::{Identity, TlsAcceptor, TlsStream};
#[cfg(feature = "rustls")]
use __rustls::{Certificate as RustlsCert, PrivateKey};
#[cfg(feature = "rustls")]
use __rustls::{ServerConfig, ServerConnection, StreamOwned};
#[cfg(any(feature = "native-tls-basic", feature = "rustls"))]
use rcgen::{Certificate, CertificateParams, KeyPair, PKCS_RSA_SHA256};
#[cfg(any(feature = "native-tls-basic", feature = "rustls"))]
use rsa::pkcs1::LineEnding;
#[cfg(any(feature = "native-tls-basic", feature = "rustls"))]
use rsa::pkcs8::EncodePrivateKey;
#[cfg(any(feature = "native-tls-basic", feature = "rustls"))]
use rsa::RsaPrivateKey;
use std::io::{Read, Write};
use std::net::TcpStream;
#[cfg(feature = "rustls")]
use std::sync::Arc;

/// A stream that might be protected with TLS.
/// This is basically a carbon copy of the one implemented by `tungstenite`,
/// except that we want to wrap a server TcpStream, not a client.
/// This wrapper serves in HTTP Transport.
pub enum MaybeTlsStream {
    /// Unencrypted socket stream.
    Plain(TcpStream),
    #[cfg(feature = "native-tls-basic")]
    /// Encrypted socket stream using `native-tls`.
    NativeTls(TlsStream<TcpStream>),
    #[cfg(feature = "rustls")]
    /// Encrypted socket stream using `rustls`.
    Rustls(StreamOwned<ServerConnection, TcpStream>),
}

impl MaybeTlsStream {
    /// Wraps the underlying stream
    #[allow(unreachable_code)]
    pub fn wrap(stream: TcpStream, encryption: bool) -> TransportResult<MaybeTlsStream> {
        match encryption {
            false => Ok(MaybeTlsStream::Plain(stream)),
            true => {
                #[cfg(feature = "native-tls-basic")]
                return Self::get_native_tls_stream(stream);

                #[cfg(feature = "rustls")]
                return Self::get_rustls_stream(stream);

                panic!("native-tls or rustls features must be enabled to use encryption")
            }
        }
    }

    #[cfg(any(feature = "rustls", feature = "native-tls-basic"))]
    fn make_cert() -> TransportResult<Certificate> {
        let mut params = CertificateParams::default();
        params.alg = &PKCS_RSA_SHA256;
        params.key_pair = Some(Self::make_rsa_keypair()?);
        Ok(Certificate::from_params(params)?)
    }

    #[cfg(any(feature = "rustls", feature = "native-tls-basic"))]
    fn make_rsa_keypair() -> TransportResult<KeyPair> {
        let mut rng = rand::thread_rng();
        let bits = 2048;
        let private_key = RsaPrivateKey::new(&mut rng, bits)?;
        let key = private_key.to_pkcs8_pem(LineEnding::CRLF)?;
        Ok(KeyPair::from_pem(&key)?)
    }

    #[cfg(feature = "native-tls-basic")]
    fn get_native_tls_stream(socket: TcpStream) -> TransportResult<MaybeTlsStream> {
        let cert = Self::make_cert()?;
        let tls_cert = cert.serialize_pem()?;
        let key = cert.serialize_private_key_pem();

        let ident = Identity::from_pkcs8(tls_cert.as_bytes(), key.as_bytes())?;
        let connector = TlsAcceptor::new(ident)?;
        let stream = connector
            .accept(socket)
            .map_err(|_| HttpTransportError::HandshakeError)?;
        Ok(MaybeTlsStream::NativeTls(stream))
    }

    #[cfg(feature = "rustls")]
    fn get_rustls_stream(socket: TcpStream) -> TransportResult<MaybeTlsStream> {
        let cert = Self::make_cert()?;
        let tls_cert = RustlsCert(cert.serialize_der()?);
        let key = PrivateKey(cert.serialize_private_key_der());

        let config = {
            Arc::new(
                ServerConfig::builder()
                    .with_safe_defaults()
                    .with_no_client_auth()
                    .with_single_cert(vec![tls_cert], key)?,
            )
        };
        let client = ServerConnection::new(config)?;
        let stream = StreamOwned::new(client, socket);

        Ok(MaybeTlsStream::Rustls(stream))
    }
}

impl Read for MaybeTlsStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            MaybeTlsStream::Plain(ref mut s) => s.read(buf),
            #[cfg(feature = "native-tls-basic")]
            MaybeTlsStream::NativeTls(ref mut s) => s.read(buf),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(ref mut s) => s.read(buf),
        }
    }
}

impl Write for MaybeTlsStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            MaybeTlsStream::Plain(ref mut s) => s.write(buf),
            #[cfg(feature = "native-tls-basic")]
            MaybeTlsStream::NativeTls(ref mut s) => s.write(buf),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            MaybeTlsStream::Plain(ref mut s) => s.flush(),
            #[cfg(feature = "native-tls-basic")]
            MaybeTlsStream::NativeTls(ref mut s) => s.flush(),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(ref mut s) => s.flush(),
        }
    }
}
