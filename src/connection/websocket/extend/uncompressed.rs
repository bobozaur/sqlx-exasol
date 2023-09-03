use async_tungstenite::{tungstenite::Message, WebSocketStream};
use futures_util::{io::BufReader, SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use sqlx_core::Error as SqlxError;

use crate::{
    connection::websocket::socket::ExaSocket,
    error::{ExaProtocolError, ExaResultExt},
    responses::Response,
};

#[derive(Debug)]
pub struct PlainWebSocket(pub WebSocketStream<BufReader<ExaSocket>>);

impl PlainWebSocket {
    /// Sends an uncompressed command.
    pub async fn send(&mut self, cmd: String) -> Result<(), SqlxError> {
        self.0.send(Message::Text(cmd)).await.to_sqlx_err()
    }

    /// Receives an uncompressed [`Response<T>`].
    pub async fn recv<T>(&mut self) -> Result<Response<T>, SqlxError>
    where
        T: DeserializeOwned,
    {
        while let Some(response) = self.0.next().await {
            let msg = response.to_sqlx_err()?;

            return match msg {
                Message::Text(s) => serde_json::from_str(&s).to_sqlx_err(),
                Message::Binary(v) => serde_json::from_slice(&v).to_sqlx_err(),
                Message::Close(c) => {
                    self.close().await.ok();
                    Err(ExaProtocolError::from(c))?
                }
                _ => continue,
            };
        }

        Err(ExaProtocolError::MissingMessage)?
    }

    pub async fn close(&mut self) -> Result<(), SqlxError> {
        self.0.close(None).await.to_sqlx_err()?;
        Ok(())
    }
}
