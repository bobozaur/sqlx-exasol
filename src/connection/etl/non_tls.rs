use std::net::SocketAddrV4;

use sqlx_core::{
    error::Error as SqlxError,
    net::{Socket, WithSocket},
};

use super::{get_etl_addr, traits::WithSocketMaker, SocketFuture};
use crate::connection::websocket::socket::WithExaSocket;

/// Implementor of [`WithSocketMaker`] used for the creation of [`WithNonTlsSocket`].
pub struct NonTlsSocketSpawner;

impl WithSocketMaker for NonTlsSocketSpawner {
    type WithSocket = WithNonTlsSocket;

    fn make_with_socket(&self, wrapper: WithExaSocket) -> Self::WithSocket {
        WithNonTlsSocket(wrapper)
    }
}

/// Newtype implemented for uniform ETL socket spawning, even
/// though without TLS there's no need to return a future.
pub struct WithNonTlsSocket(WithExaSocket);

impl WithNonTlsSocket {
    async fn work<S: Socket>(self, socket: S) -> Result<(SocketAddrV4, SocketFuture), SqlxError> {
        let (socket, address) = get_etl_addr(socket).await?;
        let future = Box::pin(async move { Ok(self.0.with_socket(socket).await) });
        Ok((address, future))
    }
}

impl WithSocket for WithNonTlsSocket {
    type Output = Result<(SocketAddrV4, SocketFuture), SqlxError>;

    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        self.work(socket).await
    }
}
