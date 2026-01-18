use std::{io, net::AddrParseError, str::Utf8Error};

use thiserror::Error as ThisError;

/// Enum representing ETL errors.
#[derive(Clone, Debug, ThisError)]
pub enum ExaEtlError {
    #[error("chunk size overflowed 64 bits")]
    ChunkSizeOverflow,
    #[error("expected HEX or CR found {0}")]
    InvalidChunkSizeByte(u8),
    #[error("expected {0} found {1}")]
    InvalidByte(u8, u8),
    #[error("failed to write the buffered data")]
    WriteZero,
    #[error("Unexpected output, a result set, returned by ETL job")]
    ResultSetFromEtl,
    #[error("Failed to parse ETL internal IP address: {0}")]
    InvalidInternalAddr(#[from] AddrParseError),
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

pub fn map_http_error(err: hyper::http::Error) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, err)
}

pub fn data_pipe_error<T>(_: T) -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "error transferring data between worker and HTTP connection",
    )
}

pub fn channel_pipe_error<T>(_: T) -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "error transferring channel between worker and HTTP connection",
    )
}
