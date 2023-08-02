use async_tungstenite::tungstenite::protocol::CloseFrame;
use async_tungstenite::tungstenite::Error as WsError;
use rsa::errors::Error as RsaError;
use serde_json::error::Error as JsonError;
use sqlx_core::Error as SqlxError;

use std::fmt::Debug;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum ExaProtocolError {
    #[error("can't fetch more rows, no result set handle provided in response")]
    MissingResultHandle,
    #[error("invalid response from database, expecting {0}")]
    UnexpectedResponse(&'static str),
    #[error("transaction already open")]
    TransactionAlreadyOpen,
    #[error("no response data received")]
    MissingResponseData,
    #[error("no message received")]
    MissingMessage,
    #[error("server closed connection due to: {0}")]
    WebsocketClosed(String),
    #[error("feature 'flate2' must be enabled to use compression")]
    CompressionDisabled,
}

impl<'a> From<Option<CloseFrame<'a>>> for ExaProtocolError {
    fn from(value: Option<CloseFrame<'a>>) -> Self {
        let msg = value
            .map(|c| c.to_string())
            .unwrap_or("unknown reason".to_owned());

        Self::WebsocketClosed(msg)
    }
}

impl From<ExaProtocolError> for SqlxError {
    fn from(value: ExaProtocolError) -> Self {
        Self::Protocol(value.to_string())
    }
}

pub(crate) trait ExaResultExt<T> {
    fn to_sqlx_err(self) -> Result<T, SqlxError>;
}

impl<T> ExaResultExt<T> for Result<T, WsError> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        let e = match self {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        let e = match e {
            WsError::ConnectionClosed => SqlxError::Protocol(WsError::ConnectionClosed.to_string()),
            WsError::AlreadyClosed => SqlxError::Protocol(WsError::AlreadyClosed.to_string()),
            WsError::Io(e) => SqlxError::Io(e),
            WsError::Tls(e) => SqlxError::Tls(e.into()),
            WsError::Capacity(e) => SqlxError::Protocol(e.to_string()),
            WsError::Protocol(e) => SqlxError::Protocol(e.to_string()),
            WsError::SendQueueFull(e) => SqlxError::Protocol(e.to_string()),
            WsError::Utf8 => SqlxError::Protocol(WsError::Utf8.to_string()),
            WsError::Url(e) => SqlxError::Configuration(e.into()),
            WsError::Http(r) => SqlxError::Protocol(format!("HTTP error: {}", r.status())),
            WsError::HttpFormat(e) => SqlxError::Protocol(e.to_string()),
        };

        Err(e)
    }
}

impl<T> ExaResultExt<T> for Result<T, RsaError> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        self.map_err(|e| SqlxError::Protocol(e.to_string()))
    }
}

impl<T> ExaResultExt<T> for Result<T, JsonError> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        self.map_err(|e| SqlxError::Protocol(e.to_string()))
    }
}
