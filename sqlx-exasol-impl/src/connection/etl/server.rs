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
