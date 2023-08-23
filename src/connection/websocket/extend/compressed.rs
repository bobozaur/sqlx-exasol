use crate::{
    error::{ExaProtocolError, ExaResultExt},
    responses::Response,
};
use async_tungstenite::WebSocketStream;
use futures_util::io::BufReader;

use async_tungstenite::tungstenite::Message;
use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use sqlx_core::Error as SqlxError;

use std::io::Write;

use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};

use crate::connection::websocket::socket::ExaSocket;

#[derive(Debug)]
pub struct CompressedWebSocket(pub WebSocketStream<BufReader<ExaSocket>>);

impl CompressedWebSocket {
    /// Compresses and sends a command.
    pub async fn send(&mut self, cmd: String) -> Result<(), SqlxError> {
        let byte_cmd = cmd.as_bytes();
        let mut compressed_cmd = Vec::new();
        let mut enc = ZlibEncoder::new(&mut compressed_cmd, Compression::default());

        enc.write_all(byte_cmd)?;
        enc.finish()?;

        self.0
            .send(Message::Binary(compressed_cmd))
            .await
            .to_sqlx_err()
    }

    /// Receives a compressed [`Response<T>`] and decompresses it.
    pub async fn recv<T>(&mut self) -> Result<Response<T>, SqlxError>
    where
        T: DeserializeOwned,
    {
        while let Some(response) = self.0.next().await {
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

    pub async fn close(&mut self) -> Result<(), SqlxError> {
        self.0.close(None).await.to_sqlx_err()?;
        Ok(())
    }
}
