use async_compression::futures::{bufread::ZlibDecoder, write::ZlibEncoder};
use async_tungstenite::{tungstenite::Message, WebSocketStream};
use futures_util::{io::BufReader, AsyncReadExt, AsyncWriteExt, SinkExt, StreamExt, TryFutureExt};
use serde::de::DeserializeOwned;
use sqlx_core::Error as SqlxError;

use crate::{
    connection::websocket::socket::ExaSocket,
    error::{ExaProtocolError, ToSqlxError},
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
        enc.close().await?;

        SinkExt::send(&mut self.0, Message::Binary(compressed_cmd.into()))
            .map_err(ToSqlxError::to_sqlx_err)
            .await
    }

    /// Receives a compressed [`Response<T>`] and decompresses it.
    pub async fn recv<T>(&mut self) -> Result<Response<T>, SqlxError>
    where
        T: DeserializeOwned,
    {
        while let Some(response) = self.0.next().await {
            let bytes = match response.map_err(ToSqlxError::to_sqlx_err)? {
                Message::Text(s) => s.into(),
                Message::Binary(v) => v,
                Message::Close(c) => {
                    self.close().await.ok();
                    Err(ExaProtocolError::from(c))?
                }
                _ => continue,
            };

            // The whole point of compression is to end up with smaller data so
            // we might as well allocate the length we know from the encoded one in advance.
            let mut decoded = Vec::with_capacity(bytes.len());
            ZlibDecoder::new(bytes.as_ref())
                .read_to_end(&mut decoded)
                .await?;

            return serde_json::from_slice(&decoded).map_err(ToSqlxError::to_sqlx_err);
        }

        Err(ExaProtocolError::MissingMessage)?
    }

    pub async fn close(&mut self) -> Result<(), SqlxError> {
        self.0.close(None).await.map_err(ToSqlxError::to_sqlx_err)?;
        Ok(())
    }
}
