use std::{
    error::Error as StdError,
    fmt::Debug,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use futures_util::{task::AtomicWaker, FutureExt};
use hyper::{
    body::{Body, Incoming},
    server::conn::http1::{Builder, Connection},
    service::HttpService,
};
use sqlx_core::rt::JoinHandle;

use crate::{
    connection::websocket::socket::ExaSocket,
    etl::{error::map_hyper_err, job::SocketHandshake},
};

pub struct OneShotHttpServer<S>
where
    S: HttpService<Incoming>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    S::ResBody: 'static,
    <S::ResBody as Body>::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    state: ServerState<S>,
    waker: Arc<AtomicWaker>,
    stop: Arc<AtomicBool>,
}

impl<S> OneShotHttpServer<S>
where
    S: HttpService<Incoming>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    S::ResBody: 'static,
    <S::ResBody as Body>::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    pub fn new(
        future: SocketHandshake,
        service: S,
        waker: Arc<AtomicWaker>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        Self {
            state: ServerState::SocketHandshake {
                future,
                service: Some(service),
            },
            waker,
            stop,
        }
    }
}

impl<S> Future for OneShotHttpServer<S>
where
    S: HttpService<Incoming> + Unpin,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    S::ResBody: 'static,
    <S::ResBody as Body>::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.stop.load(Ordering::Relaxed) {
            return Poll::Ready(Ok(()));
        }

        loop {
            match &mut self.state {
                ServerState::SocketHandshake { future, service } => {
                    let socket = match future.poll_unpin(cx) {
                        Poll::Ready(res) => res?,
                        Poll::Pending => {
                            self.waker.register(cx.waker());
                            return Poll::Pending;
                        }
                    };
                    let conn = Builder::new()
                        .keep_alive(false)
                        .serve_connection(socket, service.take().unwrap());
                    self.state = ServerState::Serving(conn);
                }
                ServerState::Serving(connection) => {
                    break match connection.poll_unpin(cx).map_err(map_hyper_err) {
                        Poll::Ready(res) => Poll::Ready(res),
                        Poll::Pending => {
                            self.waker.register(cx.waker());
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

enum ServerState<S>
where
    S: HttpService<Incoming>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    S::ResBody: 'static,
    <S::ResBody as Body>::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    SocketHandshake {
        future: SocketHandshake,
        service: Option<S>,
    },
    Serving(Connection<ExaSocket, S>),
}

pub struct ServerTask {
    pub handle: JoinHandle<io::Result<()>>,
    pub waker: Arc<AtomicWaker>,
}

impl Future for ServerTask {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.handle.poll_unpin(cx)
    }
}

impl Debug for ServerTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTask")
            .field("handle", &())
            .field("waker", &self.waker)
            .finish()
    }
}
