#[cfg(feature = "compression")]
mod compressed;
mod uncompressed;

use std::{
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use async_tungstenite::tungstenite::Message;
#[cfg(feature = "compression")]
use compressed::CompressedWebSocket;
use futures_core::Stream;
use futures_util::{Sink, SinkExt, StreamExt};
use sqlx_core::{bytes::Bytes, Error as SqlxError};
pub use uncompressed::PlainWebSocket;

use crate::error::ToSqlxError;

/// Websocket extension enum that wraps the plain and compressed variants
/// of the websocket used for a connection.
#[derive(Debug)]
pub enum MaybeCompressedWebSocket {
    Plain(PlainWebSocket),
    #[cfg(feature = "compression")]
    Compressed(CompressedWebSocket),
}

impl MaybeCompressedWebSocket {
    pub fn maybe_compress(self, use_compression: bool) -> Self {
        match (self, use_compression) {
            #[cfg(feature = "compression")]
            (Self::Plain(plain), true) => MaybeCompressedWebSocket::Compressed(plain.into()),
            (ws, _) => ws,
        }
    }

    pub async fn ping(&mut self) -> Result<(), SqlxError> {
        let ws = match self {
            MaybeCompressedWebSocket::Plain(ws) => &mut ws.0,
            #[cfg(feature = "compression")]
            MaybeCompressedWebSocket::Compressed(ws) => &mut ws.inner,
        };

        ws.send(Message::Ping(Bytes::new()))
            .await
            .map_err(ToSqlxError::to_sqlx_err)
    }

    pub fn socket_addr(&self) -> SocketAddr {
        let ws = match self {
            MaybeCompressedWebSocket::Plain(ws) => &ws.0,
            #[cfg(feature = "compression")]
            MaybeCompressedWebSocket::Compressed(ws) => &ws.inner,
        };

        ws.get_ref().get_ref().sock_addr
    }
}

impl Stream for MaybeCompressedWebSocket {
    type Item = Result<Bytes, SqlxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            MaybeCompressedWebSocket::Plain(ws) => ws.poll_next_unpin(cx),
            #[cfg(feature = "compression")]
            MaybeCompressedWebSocket::Compressed(ws) => ws.poll_next_unpin(cx),
        }
    }
}

impl Sink<String> for MaybeCompressedWebSocket {
    type Error = SqlxError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            MaybeCompressedWebSocket::Plain(ws) => ws.poll_ready_unpin(cx),
            #[cfg(feature = "compression")]
            MaybeCompressedWebSocket::Compressed(ws) => ws.poll_ready_unpin(cx),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        match self.get_mut() {
            MaybeCompressedWebSocket::Plain(ws) => ws.start_send_unpin(item),
            #[cfg(feature = "compression")]
            MaybeCompressedWebSocket::Compressed(ws) => ws.start_send_unpin(item),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            MaybeCompressedWebSocket::Plain(ws) => ws.poll_flush_unpin(cx),
            #[cfg(feature = "compression")]
            MaybeCompressedWebSocket::Compressed(ws) => ws.poll_flush_unpin(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            MaybeCompressedWebSocket::Plain(ws) => ws.poll_close_unpin(cx),
            #[cfg(feature = "compression")]
            MaybeCompressedWebSocket::Compressed(ws) => ws.poll_close_unpin(cx),
        }
    }
}
