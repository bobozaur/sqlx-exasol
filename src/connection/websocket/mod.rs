pub mod futures;
pub mod socket;
mod tls;
mod transport;

use std::{
    fmt::Debug,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;
use futures_util::{io::BufReader, Sink, SinkExt, StreamExt};
use lru::LruCache;
use socket::ExaSocket;
use sqlx_core::{bytes::Bytes, Error as SqlxError};
pub use tls::WithMaybeTlsExaSocket;
use transport::MaybeCompressedWebSocket;

use crate::{
    connection::{
        futures::{CloseResultSets, ExaLogin, GetAttributes, Rollback, WebSocketFuture},
        websocket::transport::PlainWebSocket,
    },
    error::ToSqlxError,
    request::ExaLoginRequest,
    responses::{ExaAttributes, PreparedStatement, SessionInfo},
};

/// A websocket connected to the Exasol, providing lower level
/// interaction with the database.
#[derive(Debug)]
pub struct ExaWebSocket {
    pub inner: MaybeCompressedWebSocket,
    pub attributes: ExaAttributes,
    pub pending_close: Option<CloseResultSets>,
    pub pending_rollback: Option<Rollback>,
    pub statement_cache: LruCache<String, PreparedStatement>,
    pub active_request: bool,
}

impl ExaWebSocket {
    const WS_SCHEME: &'static str = "ws";
    const WSS_SCHEME: &'static str = "wss";

    pub async fn new(
        host: &str,
        port: u16,
        socket: ExaSocket,
        options: ExaLoginRequest<'_>,
        with_tls: bool,
    ) -> Result<(Self, SessionInfo), SqlxError> {
        let scheme = if with_tls {
            Self::WSS_SCHEME
        } else {
            Self::WS_SCHEME
        };

        let host = format!("{scheme}://{host}:{port}");

        let (ws, _) = async_tungstenite::client_async(host, BufReader::new(socket))
            .await
            .map_err(ToSqlxError::to_sqlx_err)?;

        let attributes = ExaAttributes::new(
            options.use_compression,
            options.fetch_size,
            with_tls,
            options.statement_cache_capacity,
        );

        let statement_cache = LruCache::new(options.statement_cache_capacity);

        // Regardless of the compression choice, we always start uncompressed, login, then enable
        // compression, if necessary.
        let inner = MaybeCompressedWebSocket::Plain(PlainWebSocket(ws));
        let use_compression = options.use_compression;
        let mut this = Self {
            inner,
            attributes,
            pending_close: None,
            pending_rollback: None,
            statement_cache,
            active_request: false,
        };

        // Login is always uncompressed!
        let session_info = ExaLogin::new(options).future(&mut this).await?;

        // Use compression if indicated to do so and it's enabled through the feature flagged.
        this.inner = this.inner.maybe_compress(use_compression);

        GetAttributes::default().future(&mut this).await?;
        Ok((this, session_info))
    }

    pub async fn ping(&mut self) -> Result<(), SqlxError> {
        self.inner.ping().await
    }

    pub fn socket_addr(&self) -> SocketAddr {
        self.inner.socket_addr()
    }
}

impl Stream for ExaWebSocket {
    type Item = Result<Bytes, SqlxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

impl Sink<String> for ExaWebSocket {
    type Error = SqlxError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().inner.poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        self.get_mut().inner.start_send_unpin(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().inner.poll_flush_unpin(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().inner.poll_close_unpin(cx)
    }
}
