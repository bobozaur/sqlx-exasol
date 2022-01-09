use crate::connection::ExaError;
use crossbeam::channel::RecvError;
use rsa;
use serde_json;
use std::array::TryFromSliceError;
use std::fmt::Debug;
use std::net::TcpStream;
use std::num::ParseIntError;
use thiserror::Error as ThisError;
use tungstenite;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{ClientHandshake, HandshakeError};

/// Result implementation for the crate.;
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for the crate.
#[derive(Debug, ThisError)]
pub enum Error {
    #[error(transparent)]
    QueryError(#[from] QueryError),
    #[error(transparent)]
    DriverError(#[from] DriverError),
    #[error(transparent)]
    ExasolError(#[from] ExaError),
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

/// Query related errors.
#[derive(Debug, ThisError)]
pub enum QueryErrorImpl {
    #[error(transparent)]
    BindError(#[from] BindError),
    #[error(transparent)]
    QueryError(#[from] ExaError),
}

/// Error related to parameter binding.
#[derive(Debug, ThisError)]
pub enum BindError {
    #[error("Missing parameter to bind for {0}")]
    MappingError(String),
    #[error("Parameter type must serialize to a sequence or map")]
    SerializeError,
    #[error(transparent)]
    DeserializeError(#[from] serde_json::error::Error),
}

/// Implementation for an actual error occurring on a query
#[derive(Debug, ThisError)]
#[error("Error: {source:#?}\nQuery: {query}")]
pub struct QueryError {
    query: String,
    source: QueryErrorImpl,
}

impl QueryError {
    pub(crate) fn new<T>(source: QueryErrorImpl, query: T) -> Self
    where
        T: AsRef<str>,
    {
        let query = query.as_ref().to_owned();
        Self { query, source }
    }

    pub(crate) fn map_err<T>(err: Error, query: T) -> Error
    where
        T: AsRef<str>,
    {
        match err {
            Error::ExasolError(e) => Error::QueryError(QueryError::new(e.into(), query)),
            _ => err,
        }
    }
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
    TypeParseError(#[from] serde_json::error::Error),
}

/// Request related errors
#[derive(Debug, ThisError)]
pub enum RequestError {
    #[error(transparent)]
    MessageParseError(#[from] serde_json::error::Error),
    #[error(transparent)]
    WebsocketError(#[from] tungstenite::error::Error),
    #[error(transparent)]
    CompressionError(#[from] std::io::Error),
    #[error("Cannot fetch rows chunk - missing statement handle")]
    MissingHandleError,
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
    WebsocketError(#[from] tungstenite::error::Error),
    #[error(transparent)]
    HandshakeError(#[from] HandshakeError<ClientHandshake<MaybeTlsStream<TcpStream>>>),
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
    #[error(transparent)]
    CSVError(#[from] csv::Error),
    #[error("Invalid chunk delimiter in HTTP stream")]
    DelimiterError,
    #[error("Error receiving data across channel")]
    RecvError(#[from] RecvError),
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
