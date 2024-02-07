use sqlx_core::net::WithSocket;

use crate::{connection::websocket::socket::WithExaSocket, etl::WithSocketFuture};

/// Trait used as an interface for constructing a type implementing [`WithSocket`]
/// that outputs a socket spawning future.
///
/// The constructed future can then be awaited regardless of its origin, bridging the TLS/non-TLS
/// code.
pub trait WithSocketMaker {
    type WithSocket: WithSocket<Output = WithSocketFuture>;

    fn make_with_socket(&self, wrapper: WithExaSocket) -> Self::WithSocket;
}
