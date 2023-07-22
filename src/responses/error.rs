use std::fmt::Display;

use serde::Deserialize;
use sqlx_core::error::{self, ErrorKind};

/// Type representing an error directly issued by the Exasol database.
#[derive(Debug, Deserialize)]
pub struct DatabaseError {
    text: String,
    #[serde(rename = "sqlCode")]
    code: String,
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Exasol Error {}: {}", self.code, self.text)
    }
}

impl std::error::Error for DatabaseError {}
impl std::error::Error for &mut DatabaseError {}

impl error::DatabaseError for DatabaseError {
    fn message(&self) -> &str {
        &self.text
    }

    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> ErrorKind {
        ErrorKind::Other
    }
}
