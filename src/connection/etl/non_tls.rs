use sqlx_core::{
    error::Error as SqlxError,
    net::{Socket, WithSocket},
};

use crate::{
    connection::websocket::socket::WithExaSocket,
    etl::{with_socket::WithSocketMaker, WithSocketFuture},
};

/// Implementor of [`WithSocketMaker`] used for the creation of [`WithNonTlsSocket`].
pub struct NonTlsSocketSpawner;

impl WithSocketMaker for NonTlsSocketSpawner {
    type WithSocket = WithNonTlsSocket;

    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket {
        WithNonTlsSocket(with_socket)
    }
}

/// Newtype implemented for uniform ETL socket spawning, even though without TLS there's no need to
/// return a future.
pub struct WithNonTlsSocket(WithExaSocket);

impl WithSocket for WithNonTlsSocket {
    type Output = Result<WithSocketFuture, SqlxError>;

    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        let future = Box::pin(async move { Ok(self.0.with_socket(socket).await) });
        Ok(future)
    }
}
