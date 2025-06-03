use sqlx_core::net::{Socket, WithSocket};

use crate::{
    connection::websocket::socket::WithExaSocket,
    etl::{with_worker::WithWorker, WithSocketFuture},
    SqlxResult,
};

/// Implementor of [`WithWorker`] used for the creation of [`WithNonTlsSocket`].
pub struct WithNonTlsWorker;

impl WithWorker for WithNonTlsWorker {
    type WithSocket = WithNonTlsSocket;

    fn make_with_socket(&self, with_socket: WithExaSocket) -> Self::WithSocket {
        WithNonTlsSocket(with_socket)
    }
}

/// Newtype implemented for uniform ETL socket spawning, even though without TLS there's no need to
/// return a future. This makes worker setup logic easier to reason with regardless of whether TLS
/// is used.
pub struct WithNonTlsSocket(WithExaSocket);

impl WithSocket for WithNonTlsSocket {
    type Output = SqlxResult<WithSocketFuture>;

    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        let future = Box::pin(async move { Ok(self.0.with_socket(socket).await) });
        Ok(future)
    }
}
