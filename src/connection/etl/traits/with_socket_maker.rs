use std::net::SocketAddrV4;

use sqlx_core::{net::WithSocket, Error as SqlxError};

use crate::{connection::websocket::socket::WithExaSocket, etl::SocketFuture};

/// Trait used as an interface for constructing a type implementing [`WithSocket`]
/// that outputs a socket spawning future.
///
/// The constructed future can then be awaited regardless of its origin, bridging the TLS/non-TLS
/// code.
pub trait WithSocketMaker {
    type WithSocket: WithSocket<Output = Result<(SocketAddrV4, SocketFuture), SqlxError>>;

    fn make_with_socket(&self, wrapper: WithExaSocket) -> Self::WithSocket;
}
