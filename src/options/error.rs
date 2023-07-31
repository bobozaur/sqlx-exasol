use std::num::ParseIntError;

use sqlx_core::Error;
use thiserror::Error as ThisError;

#[derive(Debug, Clone, ThisError)]
pub enum ExaConfigError {
    #[error("No host provided")]
    MissingHost,
    #[error("Multiple authentication methods provided")]
    MultipleAuthMethods,
    #[error("Error parsing host range: {0}")]
    InvalidHostRange(#[from] ParseIntError),
    #[error("Invalid URL scheme: {0}")]
    InvalidUrlScheme(String),
    #[error("Invalid connection parameter: {0}")]
    InvalidParameter(&'static str),
}

impl From<ExaConfigError> for Error {
    fn from(value: ExaConfigError) -> Self {
        Self::Configuration(value.into())
    }
}
