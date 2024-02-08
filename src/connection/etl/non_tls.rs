use std::{io::Result as IoResult, net::SocketAddrV4};

use futures_core::future::BoxFuture;
use sqlx_core::{
    error::Error as SqlxError,
    net::{Socket, WithSocket},
};

use super::{get_etl_addr, traits::WithSocketMaker, SocketFuture, WithSocketFuture};
use crate::connection::websocket::socket::{ExaSocket, WithExaSocket};

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
    #[allow(clippy::unused_async)]
    async fn wrap_socket<S: Socket>(self, socket: S) -> IoResult<ExaSocket> {
        let socket = self.0.with_socket(socket);
        Ok(socket)
    }

    async fn work<S: Socket>(self, socket: S) -> Result<(SocketAddrV4, SocketFuture), SqlxError> {
        let (socket, address) = get_etl_addr(socket).await?;
        let future: BoxFuture<'_, IoResult<ExaSocket>> = Box::pin(self.wrap_socket(socket));
        Ok((address, future))
    }
}

impl WithSocket for WithNonTlsSocket {
    type Output = WithSocketFuture;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        Box::pin(self.work(socket))
    }
}
