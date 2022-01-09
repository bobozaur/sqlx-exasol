#[cfg(feature = "flate2")]
use crate::error::RequestError;
#[cfg(feature = "flate2")]
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
#[cfg(feature = "flate2")]
use std::io::Write;

use super::{ReqResult, Response};
use serde_json::Value;
use std::net::TcpStream;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

/// Represents a wrapped Websocket with possible Zlib compression enabled
pub enum MaybeCompressedWs {
    Plain(WebSocket<MaybeTlsStream<TcpStream>>),
    #[cfg(feature = "flate2")]
    Compressed(WebSocket<MaybeTlsStream<TcpStream>>),
}

impl MaybeCompressedWs {
    /// Consumes self to return a variant that might use compression
    #[allow(unreachable_code)]
    pub fn enable_compression(self, compression: bool) -> Self {
        if compression {
            #[cfg(feature = "flate2")]
            return match self {
                Self::Plain(ws) => Self::Compressed(ws),
                Self::Compressed(ws) => Self::Compressed(ws),
            };

            // Shouldn't ever reach this, but just in case:
            panic!("Compression enabled without flate2 feature!")
        } else {
            match self {
                Self::Plain(ws) => Self::Plain(ws),
                #[cfg(feature = "flate2")]
                Self::Compressed(ws) => Self::Plain(ws),
            }
        }
    }

    pub fn send(&mut self, payload: Value) -> ReqResult<()> {
        match self {
            MaybeCompressedWs::Plain(ws) => {
                ws.write_message(Message::Text(payload.to_string()))?;
                Ok(())
            }
            #[cfg(feature = "flate2")]
            MaybeCompressedWs::Compressed(ws) => {
                let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());

                let message = enc
                    .write_all(payload.to_string().as_bytes())
                    .and(enc.finish())?;

                ws.write_message(Message::Binary(message))
                    .map_err(RequestError::WebsocketError)
            }
        }
    }

    pub fn recv(&mut self) -> ReqResult<Response> {
        match self {
            MaybeCompressedWs::Plain(ws) => loop {
                break match ws.read_message()? {
                    Message::Text(resp) => Ok(serde_json::from_str::<Response>(&resp)?),
                    Message::Binary(resp) => Ok(serde_json::from_slice::<Response>(&resp)?),
                    _ => continue,
                };
            },

            #[cfg(feature = "flate2")]
            MaybeCompressedWs::Compressed(ws) => loop {
                break match ws.read_message()? {
                    Message::Text(resp) => {
                        Ok(serde_json::from_reader(ZlibDecoder::new(resp.as_bytes()))?)
                    }
                    Message::Binary(resp) => {
                        Ok(serde_json::from_reader(ZlibDecoder::new(resp.as_slice()))?)
                    }
                    _ => continue,
                };
            },
        }
    }

    pub fn as_inner(&self) -> &WebSocket<MaybeTlsStream<TcpStream>> {
        match self {
            MaybeCompressedWs::Plain(ws) => ws,
            #[cfg(feature = "flate2")]
            MaybeCompressedWs::Compressed(ws) => ws,
        }
    }

    pub fn as_inner_mut(&mut self) -> &mut WebSocket<MaybeTlsStream<TcpStream>> {
        match self {
            MaybeCompressedWs::Plain(ws) => ws,
            #[cfg(feature = "flate2")]
            MaybeCompressedWs::Compressed(ws) => ws,
        }
    }
}
