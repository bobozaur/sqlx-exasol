use async_tungstenite::tungstenite::Error as WsError;
use rsa;
use serde_json;
use serde_json::error::Error as JsonError;
use std::array::TryFromSliceError;
use std::fmt::Debug;
use std::num::ParseIntError;
use thiserror::Error as ThisError;

use crate::responses::DatabaseError;

/// Result implementation for the crate.
pub type ExaResult<T> = std::result::Result<T, Error>;

// Convenience aliases
pub(crate) type ConResult<T> = std::result::Result<T, ConnectionError>;

/// Error type for the crate.
#[derive(Debug, ThisError)]
pub enum Error {
    #[error(transparent)]
    DriverError(#[from] DriverError),
    #[error(transparent)]
    DatabaseError(#[from] DatabaseError),
}

/// Driver related errors.
///
/// These errors have nothing to do with the Exasol database itself,
/// but rather the driver having issues with the underlying websocket connection,
/// making requests, processing responses, etc.
#[derive(Debug, ThisError)]
pub enum DriverError {
    #[error(transparent)]
    DataError(#[from] DataError),
    #[error(transparent)]
    RequestError(#[from] RequestError),
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
    #[error(transparent)]
    HttpTransportError(#[from] HttpTransportError),
    #[error("Response does not contain {0}")]
    ResponseMismatch(&'static str),
}

/// Data processing related errors.
#[derive(Debug, ThisError)]
pub enum DataError {
    #[error("Missing data for column {0}")]
    MissingColumn(String),
    #[error("Expecting {0} data columns in array, found {1}")]
    IncorrectLength(usize, usize),
    #[error("Data iterator items must deserialize to sequences or maps")]
    InvalidIterType,
    #[error(transparent)]
    JsonError(#[from] JsonError),
}

/// Request related errors
#[derive(Debug, ThisError)]
pub enum RequestError {
    #[error(transparent)]
    MessageParseError(#[from] JsonError),
    #[error(transparent)]
    WebsocketError(#[from] WsError),
    #[error(transparent)]
    CompressionError(#[from] std::io::Error),
    #[error("Cannot fetch rows chunk - missing statement handle")]
    MissingHandleError,
    #[error("No data message returned for request")]
    NoMessageReturned,
    #[error("Server closed the connection")]
    Closed,
}

/// Connection related errors
#[derive(Debug, ThisError)]
pub enum ConnectionError {
    #[error("Invalid DSN provided")]
    InvalidDSN,
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Cannot parse provided hostname range in DSN")]
    RangeParseError(#[from] ParseIntError),
    #[error(transparent)]
    CryptographyError(#[from] rsa::errors::Error),
    #[error(transparent)]
    PKCS1Error(#[from] rsa::pkcs1::Error),
    #[error("The fingerprint in the DSN and the server fingerprint do not match: {0:?} - {1:?}")]
    FingerprintMismatch(String, String),
    #[error(transparent)]
    WebsocketError(#[from] WsError),
    // #[error(transparent)]
    // HandshakeError(#[from] HandshakeError<ClientHandshake<MaybeTlsStream<TcpStream>>>),
    #[error("Unsupported connector type")]
    UnsupportedConnector,
    #[cfg(feature = "native-tls-basic")]
    #[error(transparent)]
    NativeTlsError(#[from] __native_tls::HandshakeError<TcpStream>),
    #[cfg(feature = "rustls")]
    #[error(transparent)]
    InvalidDsn(#[from] __rustls::client::InvalidDnsNameError),
    #[cfg(feature = "rustls")]
    #[error(transparent)]
    RustlsError(#[from] __rustls::Error),
}

/// HTTP transport related errors
#[derive(Debug, ThisError)]
pub enum HttpTransportError {
    #[error(transparent)]
    RsaError(#[from] rsa::errors::Error),
    #[error(transparent)]
    PKCS8Error(#[from] rsa::pkcs8::Error),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Could not parse port for HTTP transport")]
    PortParseError(#[from] TryFromSliceError),
    #[error("Missing mandatory parameter: {0}")]
    MissingParameter(&'static str),
    #[error("Invalid parameter {0} - {1}")]
    InvalidParameter(&'static str, String),
    #[error("Could not parse chunk size")]
    ChunkSizeError(#[from] ParseIntError),
    // #[error(transparent)]
    // CSVError(#[from] csv::Error),
    #[error("Invalid chunk delimiter in HTTP stream")]
    DelimiterError,
    // #[error("Error receiving data across channel")]
    // RecvError(#[from] RecvError),
    #[error("Error sending data across channel")]
    SendError,
    #[error("Could not join thread")]
    ThreadError,
    #[error("Could not extract socket out of CSV writer")]
    CsvSocketError,
    #[cfg(any(feature = "rcgen"))]
    #[error(transparent)]
    CertificateError(#[from] rcgen::RcgenError),
    #[cfg(feature = "native-tls-basic")]
    #[error(transparent)]
    NativeTlsError(#[from] __native_tls::Error),
    #[cfg(feature = "native-tls-basic")]
    #[error("Error occurred during TLS handshake")]
    HandshakeError,
    #[cfg(feature = "rustls")]
    #[error(transparent)]
    RustlsError(#[from] __rustls::Error),
}
