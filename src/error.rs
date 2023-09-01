use async_tungstenite::tungstenite::protocol::CloseFrame;
use async_tungstenite::tungstenite::Error as WsError;
use rsa::errors::Error as RsaError;
use serde_json::error::Error as JsonError;
use sqlx_core::Error as SqlxError;

use std::fmt::Debug;
use thiserror::Error as ThisError;

/// Enum representing protocol implementation errors.
#[derive(Debug, ThisError)]
pub enum ExaProtocolError {
    #[error("expected {0} parameter sets; found at least a mismatch, length {1}")]
    ParameterLengthMismatch(usize, usize),
    #[error("invalid response from database, expecting {0}")]
    UnexpectedResponse(&'static str),
    #[error("transaction already open")]
    TransactionAlreadyOpen,
    #[error("no response data received")]
    MissingResponseData,
    #[error("no message received")]
    MissingMessage,
    #[error("type mismatch: expected SQL type `{0}` but was provided `{1}`")]
    DatatypeMismatch(String, String),
    #[error("server closed connection due to: {0}")]
    WebsocketClosed(String),
    #[error("feature 'compression' must be enabled to use compression")]
    CompressionDisabled,
    #[cfg(feature = "etl")]
    #[error("Unexpected output, a result set, returned by ETL job")]
    ResultSetFromEtl,
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

/// Helper trait used for converting errors from various underlying libraries to SQLx.
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
            WsError::WriteBufferFull(e) => SqlxError::Protocol(e.to_string()),
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

#[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
impl<T> ExaResultExt<T> for Result<T, rcgen::RcgenError> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        self.map_err(|e| SqlxError::Tls(e.into()))
    }
}

#[cfg(feature = "etl_native_tls")]
impl<T> ExaResultExt<T> for Result<T, native_tls::Error> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        self.map_err(|e| SqlxError::Tls(e.into()))
    }
}

#[cfg(feature = "etl_native_tls")]
impl<T, S> ExaResultExt<T> for Result<T, native_tls::HandshakeError<S>> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        self.map_err(|_| SqlxError::Tls("native_tls handshake error".into()))
    }
}

#[cfg(feature = "etl_rustls")]
impl<T> ExaResultExt<T> for Result<T, rustls::Error> {
    fn to_sqlx_err(self) -> Result<T, SqlxError> {
        self.map_err(|e| SqlxError::Tls(e.into()))
    }
}
