use std::{borrow::Cow, fmt::Display};

use serde::Deserialize;
use sqlx_core::error::{self, ErrorKind};

/// Type representing an error directly issued by the Exasol database.
#[derive(Debug, Deserialize)]
pub struct ExaDatabaseError {
    text: String,
    #[serde(rename = "sqlCode")]
    code: String,
}

impl Display for ExaDatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Exasol error {}: {}", self.code, self.text)
    }
}

impl std::error::Error for ExaDatabaseError {}
impl std::error::Error for &mut ExaDatabaseError {}

impl error::DatabaseError for ExaDatabaseError {
    fn message(&self) -> &str {
        &self.text
    }

    fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
        Some(Cow::Borrowed(&self.code))
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
        match self.code.as_str() {
            "27001" => ErrorKind::NotNullViolation,
            "27002" => ErrorKind::UniqueViolation,
            "27003" => ErrorKind::ForeignKeyViolation,
            _ => ErrorKind::Other,
        }
    }
}
