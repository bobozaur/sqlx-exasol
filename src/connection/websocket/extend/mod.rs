#[cfg(feature = "compression")]
mod compressed;
mod uncompressed;

use std::net::SocketAddr;

use async_tungstenite::{tungstenite::Message, WebSocketStream};
#[cfg(feature = "compression")]
use compressed::CompressedWebSocket;
use futures_util::io::BufReader;
use serde::de::DeserializeOwned;
use sqlx_core::{bytes::Bytes, Error as SqlxError};
pub use uncompressed::PlainWebSocket;

use super::socket::ExaSocket;
use crate::{error::ExaResultExt, responses::Response};

/// Websocket extension enum that wraps the plain and compressed variants
/// of the websocket used for a connection.
#[derive(Debug)]
pub enum WebSocketExt {
    Plain(PlainWebSocket),
    #[cfg(feature = "compression")]
    Compressed(CompressedWebSocket),
}

impl WebSocketExt {
    pub fn new(ws: WebSocketStream<BufReader<ExaSocket>>, use_compression: bool) -> Self {
        match use_compression {
            #[cfg(feature = "compression")]
            true => WebSocketExt::Compressed(CompressedWebSocket(ws)),
            _ => WebSocketExt::Plain(PlainWebSocket(ws)),
        }
    }

    pub async fn send(&mut self, cmd: String) -> Result<(), SqlxError> {
        match self {
            WebSocketExt::Plain(ws) => ws.send(cmd).await,
            #[cfg(feature = "compression")]
            WebSocketExt::Compressed(ws) => ws.send(cmd).await,
        }
    }

    pub async fn recv<T>(&mut self) -> Result<Response<T>, SqlxError>
    where
        T: DeserializeOwned,
    {
        match self {
            WebSocketExt::Plain(ws) => ws.recv().await,
            #[cfg(feature = "compression")]
            WebSocketExt::Compressed(ws) => ws.recv().await,
        }
    }

    pub async fn close(&mut self) -> Result<(), SqlxError> {
        match self {
            WebSocketExt::Plain(ws) => ws.close().await,
            #[cfg(feature = "compression")]
            WebSocketExt::Compressed(ws) => ws.close().await,
        }
    }

    pub fn socket_addr(&self) -> SocketAddr {
        match self {
            WebSocketExt::Plain(ws) => ws.0.get_ref().get_ref().sock_addr,
            #[cfg(feature = "compression")]
            WebSocketExt::Compressed(ws) => ws.0.get_ref().get_ref().sock_addr,
        }
    }

    pub async fn ping(&mut self) -> Result<(), SqlxError> {
        let ws = match self {
            WebSocketExt::Plain(ws) => &mut ws.0,
            #[cfg(feature = "compression")]
            WebSocketExt::Compressed(ws) => &mut ws.0,
        };

        ws.send(Message::Ping(Bytes::new())).await.to_sqlx_err()?;
        Ok(())
    }
}
