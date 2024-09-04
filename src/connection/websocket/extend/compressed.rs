use async_compression::futures::{bufread::ZlibDecoder, write::ZlibEncoder};
use async_tungstenite::{tungstenite::Message, WebSocketStream};
use futures_util::{io::BufReader, AsyncReadExt, AsyncWriteExt, SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use sqlx_core::Error as SqlxError;

use crate::{
    connection::websocket::socket::ExaSocket,
    error::{ExaProtocolError, ExaResultExt},
    responses::Response,
};

#[derive(Debug)]
pub struct CompressedWebSocket(pub WebSocketStream<BufReader<ExaSocket>>);

impl CompressedWebSocket {
    /// Compresses and sends a command.
    pub async fn send(&mut self, cmd: String) -> Result<(), SqlxError> {
        let byte_cmd = cmd.as_bytes();
        let mut compressed_cmd = Vec::new();
        let mut enc = ZlibEncoder::new(&mut compressed_cmd);

        enc.write_all(byte_cmd).await?;

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

            // The whole point of compression is to end up with have smaller data
            // we might as well allocate the length we know from the encoded one in advance.
            let mut decoded = Vec::with_capacity(bytes.len());
            ZlibDecoder::new(bytes.as_slice())
                .read_to_end(&mut decoded)
                .await?;

            return serde_json::from_slice(&decoded).to_sqlx_err();
        }

        Err(ExaProtocolError::MissingMessage)?
    }

    pub async fn close(&mut self) -> Result<(), SqlxError> {
        self.0.close(None).await.to_sqlx_err()?;
        Ok(())
    }
}
