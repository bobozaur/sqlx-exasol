use std::fmt::{Debug, Display};

use async_tungstenite::tungstenite::{protocol::CloseFrame, Error as WsError};
use rsa::errors::Error as RsaError;
use serde_json::error::Error as JsonError;
use thiserror::Error as ThisError;

use crate::{type_info::DataTypeName, SqlxError};

/// Enum representing protocol implementation errors.
#[derive(Debug, ThisError)]
pub enum ExaProtocolError {
    #[error("JSON error: {0}")]
    Json(#[from] JsonError),
    #[error("expected {0} parameter sets; found a mismatch of length {1}")]
    ParameterLengthMismatch(usize, usize),
    #[error("transaction already open")]
    TransactionAlreadyOpen,
    #[error("not ready to send data")]
    SendNotReady,
    #[error("no response received")]
    NoResponse,
    #[error("type mismatch: expected SQL type `{0}` but was provided `{1}`")]
    DatatypeMismatch(DataTypeName, DataTypeName),
    #[error("server closed connection; info: {0}")]
    WebSocketClosed(CloseError),
    #[error("feature 'compression' must be enabled to use compression")]
    CompressionDisabled,
}

#[derive(Debug)]
pub struct CloseError(Option<CloseFrame>);

impl Display for CloseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Some(c) => write!(f, "{c}"),
            None => write!(f, "unknown reason"),
        }
    }
}

impl From<Option<CloseFrame>> for ExaProtocolError {
    fn from(value: Option<CloseFrame>) -> Self {
        Self::WebSocketClosed(CloseError(value))
    }
}

impl From<ExaProtocolError> for SqlxError {
    fn from(value: ExaProtocolError) -> Self {
        Self::Protocol(value.to_string())
    }
}

/// Helper trait used for converting errors from various underlying libraries to `SQLx`.
pub trait ToSqlxError {
    fn to_sqlx_err(self) -> SqlxError;
}

impl ToSqlxError for WsError {
    fn to_sqlx_err(self) -> SqlxError {
        match self {
            WsError::ConnectionClosed => SqlxError::Protocol(WsError::ConnectionClosed.to_string()),
            WsError::AlreadyClosed => SqlxError::Protocol(WsError::AlreadyClosed.to_string()),
            WsError::Io(e) => SqlxError::Io(e),
            WsError::Tls(e) => SqlxError::Tls(e.into()),
            WsError::Capacity(e) => SqlxError::Protocol(e.to_string()),
            WsError::Protocol(e) => SqlxError::Protocol(e.to_string()),
            WsError::WriteBufferFull(e) => SqlxError::Protocol(e.to_string()),
            WsError::Utf8 => SqlxError::Protocol(WsError::Utf8.to_string()),
            WsError::Url(e) => SqlxError::Configuration(e.into()),
            WsError::Http(r) => SqlxError::Protocol(format!("HTTP error: {}", r.status())),
            WsError::HttpFormat(e) => SqlxError::Protocol(e.to_string()),
            WsError::AttackAttempt => SqlxError::Tls(WsError::AttackAttempt.into()),
        }
    }
}

impl ToSqlxError for RsaError {
    fn to_sqlx_err(self) -> SqlxError {
        SqlxError::Protocol(self.to_string())
    }
}
