use std::net::SocketAddrV4;

use futures_core::future::BoxFuture;
use sqlx_core::{net::WithSocket, Error as SqlxError};

use crate::{connection::websocket::socket::WithExaSocket, etl::SocketFuture};

pub trait SocketSpawner {
    type WithSocket: WithSocket<
        Output = BoxFuture<'static, Result<(SocketAddrV4, SocketFuture), SqlxError>>,
    >;

    fn make_with_socket(&self, wrapper: WithExaSocket) -> Self::WithSocket;
}
