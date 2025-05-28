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
    connection::futures::{GetAttributes, WebSocketFuture},
    error::ToSqlxError,
    options::ExaConnectOptionsRef,
    responses::{ExaAttributes, PreparedStatement, SessionInfo},
};

/// A websocket connected to the Exasol, providing lower level
/// interaction with the database.
#[derive(Debug)]
pub struct ExaWebSocket {
    pub ws: MaybeCompressedWebSocket,
    pub attributes: ExaAttributes,
    pub last_rs_handle: Option<u16>,
    pub statement_cache: LruCache<String, PreparedStatement>,
}

impl ExaWebSocket {
    const WS_SCHEME: &'static str = "ws";
    const WSS_SCHEME: &'static str = "wss";

    pub async fn new(
        host: &str,
        port: u16,
        socket: ExaSocket,
        options: ExaConnectOptionsRef<'_>,
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
            options.compression,
            options.fetch_size,
            with_tls,
            options.statement_cache_capacity,
        );

        let statement_cache = LruCache::new(options.statement_cache_capacity);
        let (ws, session_info) =
            MaybeCompressedWebSocket::new(ws, attributes.compression_enabled(), options).await?;

        let mut this = Self {
            ws,
            attributes,
            last_rs_handle: None,
            statement_cache,
        };

        GetAttributes::default().future(&mut this).await?;

        Ok((this, session_info))
    }

    pub async fn ping(&mut self) -> Result<(), SqlxError> {
        self.ws.ping().await
    }

    pub fn socket_addr(&self) -> SocketAddr {
        self.ws.socket_addr()
    }
}

impl Stream for ExaWebSocket {
    type Item = Result<Bytes, SqlxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().ws.poll_next_unpin(cx)
    }
}

impl Sink<String> for ExaWebSocket {
    type Error = SqlxError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        self.get_mut().start_send_unpin(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_flush_unpin(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_close_unpin(cx)
    }
}
