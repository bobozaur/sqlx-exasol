use std::{error::Error as StdError, io};

use flume::{r#async::RecvFut, Receiver, Sender};
use futures_util::{
    self,
    future::{self, Either},
    FutureExt,
};
use hyper::{
    body::{Body, Incoming},
    server::conn::http1::{Builder, Connection},
    service::HttpService,
};
use sqlx_core::net::{Socket, WithSocket};

use crate::{
    connection::websocket::socket::ExaSocket,
    etl::{error::map_hyper_err, job::ServerBootstrap},
};

/// A `WithSocket` implementation that wraps another `WithSocket` to layer a one-shot HTTP
/// server on top of the established socket.
///
/// This struct is central to the ETL server bootstrap process. It is responsible for:
/// 1. Establishing a connection using the inner `WithSocket`.
/// 2. Creating an HTTP server ([`hyper::server::conn::http1::Connection`]).
/// 3. Racing the HTTP server against receiving a data channel from the
///    [`hyper::service::HttpService`]. The data channel is sent by the service when it receives a
///    request from Exasol.
/// 4. Sending the data channel and the paused HTTP connection to the IO worker, which will then
///    take over polling the connection.
#[derive(Debug)]
pub struct WithHttpServer<WS, SERVICE, CHANNEL>
where
    WS: WithSocket,
    CHANNEL: Send + 'static,
    SERVICE: OneShotService,
{
    inner: WS,
    service: SERVICE,
    recv: RecvFut<'static, CHANNEL>,
    sender: Sender<(CHANNEL, Connection<ExaSocket, SERVICE>)>,
}

impl<WS, SERVICE, CHANNEL> WithHttpServer<WS, SERVICE, CHANNEL>
where
    WS: WithSocket,
    CHANNEL: Send + 'static,
    SERVICE: OneShotService,
{
    pub fn new(
        with_socket: WS,
        service: SERVICE,
        rx: Receiver<CHANNEL>,
        tx: Sender<(CHANNEL, Connection<ExaSocket, SERVICE>)>,
    ) -> Self {
        Self {
            inner: with_socket,
            service,
            recv: rx.into_recv_async(),
            sender: tx,
        }
    }
}

impl<WS, SERVICE, CHANNEL> WithSocket for WithHttpServer<WS, SERVICE, CHANNEL>
where
    WS: WithSocket<Output = io::Result<ExaSocket>> + Send + 'static,
    CHANNEL: Send + 'static,
    SERVICE: OneShotService,
{
    type Output = ServerBootstrap;

    /// The main logic for the server bootstrap.
    ///
    /// This method performs the following steps:
    /// 1. Awaits the inner `with_socket` future to get a connected and TLS-handshaked socket.
    /// 2. Creates an HTTP connection to serve the provided `service`.
    /// 3. Races the `Connection` future against a future waiting to receive the data channel from
    ///    the HTTP service. The service sends this channel when a request is received.
    /// 4. If the data channel is received first, it sends the channel and the paused `Connection`
    ///    to the IO worker.
    /// 5. If the `Connection` future completes first, it's an error because it's expected to be a
    ///    long-running task driven by the worker.
    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        async {
            let socket = self.inner.with_socket(socket).await?;
            let conn = Builder::new()
                .keep_alive(false)
                .serve_connection(socket, self.service);

            let (chan, conn) = match future::select(conn, self.recv).await {
                Either::Right((Ok(chan), conn)) => Ok((chan, conn)),
                Either::Right((Err(_), _)) => Err(data_channel_recv_error()),
                Either::Left((res, _)) => res
                    .map_err(map_hyper_err)
                    .and_then(|()| Err(early_finish_error())),
            }?;

            self.sender
                .into_send_async((chan, conn))
                .await
                .map_err(|_| parts_send_error())
        }
        .boxed()
    }
}

/// A marker trait for `hyper` services compatible with the one-shot ETL server.
pub trait OneShotService:
    HttpService<
        Incoming,
        Future: Send,
        Error: Into<Box<dyn StdError + Send + Sync>>,
        ResBody: OneShotResBody,
    > + Send
    + 'static
{
}

impl<S> OneShotService for S
where
    S: HttpService<Incoming> + Send + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    S::ResBody: OneShotResBody,
{
}

/// A marker trait for `hyper` body types compatible with the one-shot ETL server.
pub trait OneShotResBody:
    Body<Data: Send, Error: Into<Box<dyn StdError + Send + Sync>>> + Send + 'static
{
}

impl<B> OneShotResBody for B
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
}

fn early_finish_error() -> io::Error {
    io::Error::other("HTTP server finished early without piping data to/from IO worker")
}

fn data_channel_recv_error() -> io::Error {
    io::Error::other("error receiving the data channel from the HTTP server")
}

fn parts_send_error() -> io::Error {
    io::Error::other("error sending the HTTP server and data channel to worker")
}
