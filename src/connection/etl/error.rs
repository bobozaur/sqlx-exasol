use std::io::{Error as IoError, ErrorKind as IoErrorKind};

use thiserror::Error as ThisError;

/// Enum representing ETL errors.
#[derive(Debug, ThisError)]
pub enum ExaEtlError {
    #[error("chunk size overflowed 64 bits")]
    ChunkSizeOverflow,
    #[error("expected HEX or CR found {0}")]
    InvalidChunkSizeByte(u8),
    #[error("expected {0} found {1}")]
    InvalidByte(u8, u8),
    #[error("failed to write the buffered data")]
    WriteZero,
}

impl From<ExaEtlError> for IoError {
    fn from(value: ExaEtlError) -> Self {
        let kind = match &value {
            ExaEtlError::ChunkSizeOverflow
            | ExaEtlError::InvalidChunkSizeByte(_)
            | ExaEtlError::InvalidByte(_, _) => IoErrorKind::InvalidData,
            ExaEtlError::WriteZero => IoErrorKind::WriteZero,
        };

        IoError::new(kind, value)
    }
}
