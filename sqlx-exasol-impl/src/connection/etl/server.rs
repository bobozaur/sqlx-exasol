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

use futures_util::{
    future::{maybe_done, MaybeDone},
    task::AtomicWaker,
    FutureExt,
};
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

/// A future that handles a single HTTP connection for an ETL job.
///
/// This server will first await the `SocketHandshake` future to get a connected and
/// TLS-handshaked socket. Then, it will serve a single HTTP connection on that socket.
///
/// The server is designed to be "one-shot," meaning it will handle one request and then
/// terminate. This is because Exasol makes one HTTP request per worker.
///
/// The `stop` signal is used to gracefully shut down the server if the [`super::EtlQuery`] future
/// is dropped.
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

/// The internal state of the `OneShotHttpServer`.
enum ServerState<S>
where
    S: HttpService<Incoming>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    S::ResBody: 'static,
    <S::ResBody as Body>::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    /// The server is waiting for the socket handshake to complete.
    SocketHandshake {
        future: SocketHandshake,
        service: Option<S>,
    },
    /// The server is actively serving an HTTP connection.
    Serving(Connection<ExaSocket, S>),
}

/// A handle to a spawned [`OneShotHttpServer`] task.
///
/// This struct holds the [`JoinHandle`] of the spawned server task and a waker that
/// can be used to wake up the task.
///
/// The [`super::EtlQuery`] future uses this to poll the server tasks and ensure they complete or
/// terminate them early in case of an error.
pub struct ServerTask {
    handle: MaybeDone<JoinHandle<io::Result<()>>>,
    waker: Arc<AtomicWaker>,
}

impl ServerTask {
    pub fn new(handle: JoinHandle<io::Result<()>>, waker: Arc<AtomicWaker>) -> Self {
        Self {
            handle: maybe_done(handle),
            waker,
        }
    }

    /// Wakes up the server task.
    pub fn wake(&self) {
        self.waker.wake();
    }

    /// Takes the output of the completed server task.
    ///
    /// # Panics
    ///
    /// This will panic if the task has not completed yet.
    pub fn take_output(mut self) -> io::Result<()> {
        Pin::new(&mut self.handle).take_output().unwrap()
    }
}

impl Future for ServerTask {
    type Output = ();

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
