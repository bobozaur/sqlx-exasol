#[cfg(feature = "compression")]
mod compressed;
mod uncompressed;

use std::{
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use async_tungstenite::{tungstenite::Message, WebSocketStream};
#[cfg(feature = "compression")]
use compressed::CompressedWebSocket;
use futures_core::Stream;
use futures_util::{io::BufReader, Sink, SinkExt, StreamExt};
use sqlx_core::{bytes::Bytes, Error as SqlxError};
pub use uncompressed::PlainWebSocket;

use super::socket::ExaSocket;
use crate::{error::ToSqlxError, options::ExaConnectOptionsRef, SessionInfo};

/// Websocket extension enum that wraps the plain and compressed variants
/// of the websocket used for a connection.
#[derive(Debug)]
pub enum MaybeCompressedWebSocket {
    Plain(PlainWebSocket),
    #[cfg(feature = "compression")]
    Compressed(CompressedWebSocket),
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

impl MaybeCompressedWebSocket {
    pub async fn new(
        ws: WebSocketStream<BufReader<ExaSocket>>,
        use_compression: bool,
        opts: ExaConnectOptionsRef<'_>,
    ) -> Result<(Self, SessionInfo), SqlxError> {
        // Login in ALWAYS uncompressed!
        let (plain, session_info) = PlainWebSocket::new(ws, opts).await?;

        let ws = match use_compression {
            #[cfg(feature = "compression")]
            true => MaybeCompressedWebSocket::Compressed(plain.into()),
            _ => MaybeCompressedWebSocket::Plain(plain),
        };

        Ok((ws, session_info))
    }

    pub async fn ping(&mut self) -> Result<(), SqlxError> {
        let ws = match self {
            MaybeCompressedWebSocket::Plain(ws) => &mut ws.0,
            #[cfg(feature = "compression")]
            MaybeCompressedWebSocket::Compressed(ws) => &mut ws.inner,
        };

        SinkExt::send(ws, Message::Ping(Bytes::new()))
            .await
            .map_err(ToSqlxError::to_sqlx_err)?;
        Ok(())
    }

    pub fn socket_addr(&self) -> SocketAddr {
        match self {
            MaybeCompressedWebSocket::Plain(ws) => ws.0.get_ref().get_ref().sock_addr,
            #[cfg(feature = "compression")]
            MaybeCompressedWebSocket::Compressed(ws) => ws.inner.get_ref().get_ref().sock_addr,
        }
    }
}
