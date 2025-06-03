use std::{borrow::Cow, error::Error, fmt::Display};

use serde::Deserialize;
use sqlx_core::error::{DatabaseError, ErrorKind};

/// An error directly issued by the Exasol database. Implementor of [`DatabaseError`].
// Represents the [`super::ExaResult::Error`] variant.
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

impl Error for ExaDatabaseError {}

impl DatabaseError for ExaDatabaseError {
    fn message(&self) -> &str {
        &self.text
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.code))
    }

    fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
        self
    }

    /// Uniqueness is only available to PRIMARY KEY columns in Exasol.
    /// Additionally, there's no distinction between the PRIMARY and FOREIGN key
    /// constraint violation codes, so we have to rely on the message as well.
    ///
    /// Furthermore, there's no CHECK constraint in Exasol.
    fn kind(&self) -> ErrorKind {
        match self.code.as_str() {
            "27001" => ErrorKind::NotNullViolation,
            "42X91" if self.text.contains("primary key") => ErrorKind::UniqueViolation,
            "42X91" if self.text.contains("foreign key") => ErrorKind::ForeignKeyViolation,
            _ => ErrorKind::Other,
        }
    }
}
