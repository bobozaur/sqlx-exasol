use std::{io::Result as IoResult, net::SocketAddrV4};

use futures_core::future::BoxFuture;
use sqlx_core::{
    error::Error as SqlxError,
    net::{Socket, WithSocket},
};

use super::{get_etl_addr, traits::SocketSpawner, SocketFuture};
use crate::connection::websocket::socket::{ExaSocket, WithExaSocket};

pub struct NonTlsSocketSpawner;

impl SocketSpawner for NonTlsSocketSpawner {
    type WithSocket = WithNonTlsSocket;

    fn make_with_socket(&self, wrapper: WithExaSocket) -> Self::WithSocket {
        WithNonTlsSocket(wrapper)
    }
}

/// Newtype implemented for uniform ETL socket spawning, even
/// though without TLS there's no need to return a future.
pub struct WithNonTlsSocket(WithExaSocket);

impl WithSocket for WithNonTlsSocket {
    type Output = BoxFuture<'static, Result<(SocketAddrV4, SocketFuture), SqlxError>>;

    fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        let wrapper = self.0;

        Box::pin(async move {
            let (socket, address) = get_etl_addr(socket).await?;

            let future: BoxFuture<'_, IoResult<ExaSocket>> = Box::pin(async move {
                let socket = wrapper.with_socket(socket);
                Ok(socket)
            });

            Ok((address, future))
        })
    }
}
