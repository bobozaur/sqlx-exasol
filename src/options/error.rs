use std::num::ParseIntError;

use crate::options::URL_SCHEME;
use sqlx_core::Error as SqlxError;
use thiserror::Error as ThisError;

#[derive(Debug, Clone, ThisError)]
pub enum ExaConfigError {
    #[error("no host provided")]
    MissingHost,
    #[error("multiple authentication methods provided")]
    MultipleAuthMethods,
    #[error("error parsing host range: {0}")]
    InvalidHostRange(#[from] ParseIntError),
    #[error("invalid URL scheme: {0}, expected: {}", URL_SCHEME)]
    InvalidUrlScheme(String),
    #[error("invalid connection parameter: {0}")]
    InvalidParameter(&'static str),
}

impl From<ExaConfigError> for SqlxError {
    fn from(value: ExaConfigError) -> Self {
        Self::Configuration(value.into())
    }
}
