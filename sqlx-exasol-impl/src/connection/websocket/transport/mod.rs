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
use sqlx_core::bytes::Bytes;
pub use uncompressed::PlainWebSocket;

use crate::{error::ToSqlxError, SqlxError, SqlxResult};

/// Websocket extension enum that wraps the plain and compressed variants of the websocket used for
/// a connection.
#[derive(Debug)]
pub enum MaybeCompressedWebSocket {
    Plain(PlainWebSocket),
    #[cfg(feature = "compression")]
    Compressed(CompressedWebSocket),
}

impl MaybeCompressedWebSocket {
    /// Consumes `self` to output a possibly different variant, depending on whether compression is
    /// wanted and enabled.
    #[allow(unused_variables, reason = "conditionally compiled")]
    pub fn maybe_compress(self, use_compression: bool) -> Self {
        match self {
            #[cfg(feature = "compression")]
            Self::Plain(plain) if use_compression => {
                MaybeCompressedWebSocket::Compressed(plain.into())
            }
            ws => ws,
        }
    }

    pub async fn ping(&mut self) -> SqlxResult<()> {
        let ws = match self {
            MaybeCompressedWebSocket::Plain(ws) => &mut ws.0,
            #[cfg(feature = "compression")]
            MaybeCompressedWebSocket::Compressed(ws) => &mut ws.inner,
        };

        ws.send(Message::Ping(Bytes::new()))
            .await
            .map_err(ToSqlxError::to_sqlx_err)
    }

    pub fn server(&self) -> SocketAddr {
        let ws = match self {
            MaybeCompressedWebSocket::Plain(ws) => &ws.0,
            #[cfg(feature = "compression")]
            MaybeCompressedWebSocket::Compressed(ws) => &ws.inner,
        };

        ws.get_ref().get_ref().server
    }
}

impl Stream for MaybeCompressedWebSocket {
    type Item = SqlxResult<Bytes>;

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
