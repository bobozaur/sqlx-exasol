use std::{io, net::AddrParseError, str::Utf8Error};

use thiserror::Error as ThisError;

/// The error type for ETL operations.
#[derive(Clone, Debug, ThisError)]
pub enum ExaEtlError {
    /// The chunk size in the HTTP response overflowed 64 bits.
    #[error("chunk size overflowed 64 bits")]
    ChunkSizeOverflow,
    /// An invalid byte was encountered while parsing a chunk size.
    #[error("expected HEX or CR found {0}")]
    InvalidChunkSizeByte(u8),
    /// An unexpected byte was found in the input stream.
    #[error("expected {0} found {1}")]
    InvalidByte(u8, u8),
    /// A write operation returned `Ok(0)`, indicating that no progress was made.
    #[error("failed to write the buffered data")]
    WriteZero,
    /// An ETL query unexpectedly returned a result set instead of a row count.
    #[error("Unexpected output, a result set, returned by ETL job")]
    ResultSetFromEtl,
    /// Failed to parse the internal IP address provided by Exasol for ETL.
    #[error("Failed to parse ETL internal IP address: {0}")]
    InvalidInternalAddr(#[from] AddrParseError),
    /// The internal IP address buffer from Exasol was not valid UTF-8.
    #[error("ETL IP address buffer is not UTF-8: {0}")]
    InternalAddrNotUtf8(#[from] Utf8Error),
}

impl From<ExaEtlError> for io::Error {
    fn from(value: ExaEtlError) -> Self {
        let kind = match &value {
            ExaEtlError::ChunkSizeOverflow
            | ExaEtlError::InvalidChunkSizeByte(_)
            | ExaEtlError::InvalidByte(_, _)
            | ExaEtlError::ResultSetFromEtl
            | ExaEtlError::InvalidInternalAddr(_)
            | ExaEtlError::InternalAddrNotUtf8(_) => io::ErrorKind::InvalidData,
            ExaEtlError::WriteZero => io::ErrorKind::WriteZero,
        };

        io::Error::new(kind, value)
    }
}

/// Maps a [`hyper::Error`] to an [`io::Error`].
pub fn map_hyper_err(err: hyper::Error) -> io::Error {
    let kind = if err.is_timeout() {
        io::ErrorKind::TimedOut
    } else if err.is_parse_too_large() {
        io::ErrorKind::InvalidData
    } else {
        io::ErrorKind::BrokenPipe
    };

    io::Error::new(kind, err)
}

/// Maps a [`hyper::http::Error`] to an [`io::Error`].
pub fn map_http_error(err: hyper::http::Error) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, err)
}

/// Creates a new [`io::Error`] for a data pipe error between a worker and the HTTP connection.
pub fn data_pipe_error<T>(_: T) -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "error transferring data between worker and HTTP connection",
    )
}

/// Creates a new [`io::Error`] for a channel pipe error between a worker and the HTTP connection.
pub fn channel_pipe_error<T>(_: T) -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "error transferring channel between worker and HTTP connection",
    )
}
