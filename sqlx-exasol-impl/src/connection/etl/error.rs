use std::{io, net::AddrParseError};

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
}

impl From<ExaEtlError> for io::Error {
    fn from(value: ExaEtlError) -> Self {
        let kind = match &value {
            ExaEtlError::ChunkSizeOverflow
            | ExaEtlError::InvalidChunkSizeByte(_)
            | ExaEtlError::InvalidByte(_, _)
            | ExaEtlError::ResultSetFromEtl
            | ExaEtlError::InvalidInternalAddr(_) => io::ErrorKind::InvalidData,
            ExaEtlError::WriteZero => io::ErrorKind::WriteZero,
        };

        io::Error::new(kind, value)
    }
}
