#[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
pub mod tls;

use futures_util::FutureExt;
use sqlx_core::net::{Socket, WithSocket};

use crate::{
    connection::websocket::socket::WithExaSocket,
    etl::job::{SocketSetup, WithSocketMaker},
    SqlxError, SqlxResult,
};

/// Implementor of [`WithSocketMaker`] that abstracts away the TLS/non-TLS socket creation.
pub enum WithMaybeTlsSocketMaker {
    NonTls,
    #[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
    Tls(tls::WithTlsSocketMaker),
}

impl WithMaybeTlsSocketMaker {
    pub fn new(with_tls: bool) -> SqlxResult<Self> {
        match with_tls {
            #[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
            true => tls::with_worker().map(Self::Tls),
            #[allow(unreachable_patterns, reason = "reachable with no TLS feature ")]
            true => Err(SqlxError::Tls("No ETL TLS feature set".into())),
            false => Ok(Self::NonTls),
        }
    }
}

impl WithSocketMaker for WithMaybeTlsSocketMaker {
    type WithSocket = WithMaybeTlsSocket;

    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket {
        match self {
            Self::NonTls => WithMaybeTlsSocket::NonTls(with_socket),
            #[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
            Self::Tls(w) => WithMaybeTlsSocket::Tls(w.make_with_socket(with_socket)),
        }
    }
}

/// Implementor of [`WithSocket`] that abstracts away the TLS/non-TLS socket creation.
pub enum WithMaybeTlsSocket {
    NonTls(WithExaSocket),
    #[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
    Tls(tls::WithTlsSocket),
}

impl WithSocket for WithMaybeTlsSocket {
    type Output = SocketSetup;

    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        match self {
            WithMaybeTlsSocket::NonTls(w) => w.with_socket(socket).map(Ok).boxed(),
            #[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
            WithMaybeTlsSocket::Tls(w) => w.with_socket(socket).await,
        }
    }
}
