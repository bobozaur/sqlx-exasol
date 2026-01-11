use thiserror::Error as ThisError;

use crate::{options::URL_SCHEME, SqlxError};

// Error returned for configuration failures caused by invalid URL connection strings.
#[derive(Debug, ThisError)]
pub enum ExaConfigError {
    #[error("no host provided")]
    MissingHost,
    #[error("could not resolve hostname")]
    CouldNotResolve(#[from] std::io::Error),
    #[error("no authentication method provided")]
    MissingAuthMethod,
    #[error("multiple authentication methods provided")]
    MultipleAuthMethods,
    #[error("invalid URL scheme: {0}, expected: {URL_SCHEME}")]
    InvalidUrlScheme(String),
    #[error("invalid connection parameter: {0}")]
    InvalidParameter(&'static str),
}

impl From<ExaConfigError> for SqlxError {
    fn from(value: ExaConfigError) -> Self {
        Self::Configuration(value.into())
    }
}
