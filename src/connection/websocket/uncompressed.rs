use crate::{
    error::{ExaProtocolError, ExaResultExt},
    responses::Response,
};

use async_tungstenite::tungstenite::Message;
use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use sqlx_core::Error as SqlxError;

use super::ExaWebSocket;

impl ExaWebSocket {
    /// Sends an uncompressed command.
    pub(crate) async fn send_uncompressed(&mut self, cmd: String) -> Result<(), SqlxError> {
        self.ws.send(Message::Text(cmd)).await.to_sqlx_err()
    }

    /// Receives an uncompressed [`Response<T>`].
    pub(crate) async fn recv_uncompressed<T>(&mut self) -> Result<Response<T>, SqlxError>
    where
        T: DeserializeOwned,
    {
        while let Some(response) = self.ws.next().await {
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
}
