use crate::{
    error::{ExaProtocolError, ExaResultExt},
    responses::Response,
};

use super::ExaWebSocket;
use async_tungstenite::tungstenite::Message;
use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use sqlx_core::Error as SqlxError;

use std::io::Write;

use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};

impl ExaWebSocket {
    /// Compresses and sends a command.
    pub(crate) async fn send_compressed(&mut self, cmd: String) -> Result<(), SqlxError> {
        let byte_cmd = cmd.as_bytes();
        let mut compressed_cmd = Vec::new();
        let mut enc = ZlibEncoder::new(&mut compressed_cmd, Compression::default());

        enc.write_all(byte_cmd).and_then(|_| enc.finish())?;

        self.ws
            .send(Message::Binary(compressed_cmd))
            .await
            .to_sqlx_err()
    }

    /// Receives a compressed [`Response<T>`] and decompresses it.
    pub(crate) async fn recv_compressed<T>(&mut self) -> Result<Response<T>, SqlxError>
    where
        T: DeserializeOwned,
    {
        while let Some(response) = self.ws.next().await {
            let bytes = match response.to_sqlx_err()? {
                Message::Text(s) => s.into_bytes(),
                Message::Binary(v) => v,
                Message::Close(c) => {
                    self.close().await.ok();
                    Err(ExaProtocolError::from(c))?
                }
                _ => continue,
            };

            let dec = ZlibDecoder::new(bytes.as_slice());
            return serde_json::from_reader(dec).to_sqlx_err();
        }

        Err(ExaProtocolError::MissingMessage)?
    }
}
