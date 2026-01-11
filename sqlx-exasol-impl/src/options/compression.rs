use std::str::FromStr;

use super::{error::ExaConfigError, COMPRESSION};

/// Options for controlling the desired compression behavior of the connection to the Exasol server.
///
/// It is used by [`crate::options::builder::ExaConnectOptionsBuilder::compression_mode`].
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum ExaCompressionMode {
    /// Establish an uncompressed connection.
    Disabled,

    /// Establish a compressed connection if the compression feature is enabled, falling back to an
    /// uncompressed connection if it's not.
    ///
    /// This is the default if `compression` is not specified.
    #[default]
    Preferred,

    /// Establish a compressed connection if the compression feature is enabled.
    /// The connection attempt fails if a compressed connection cannot be established.
    Required,
}

impl ExaCompressionMode {
    const DISABLED: &str = "disabled";
    const PREFERRED: &str = "preferred";
    const REQUIRED: &str = "required";
}

impl FromStr for ExaCompressionMode {
    type Err = ExaConfigError;

    fn from_str(s: &str) -> Result<Self, ExaConfigError> {
        Ok(match &*s.to_ascii_lowercase() {
            Self::DISABLED => ExaCompressionMode::Disabled,
            Self::PREFERRED => ExaCompressionMode::Preferred,
            Self::REQUIRED => ExaCompressionMode::Required,
            _ => Err(ExaConfigError::InvalidParameter(COMPRESSION))?,
        })
    }
}

impl AsRef<str> for ExaCompressionMode {
    fn as_ref(&self) -> &str {
        match self {
            ExaCompressionMode::Disabled => Self::DISABLED,
            ExaCompressionMode::Preferred => Self::PREFERRED,
            ExaCompressionMode::Required => Self::REQUIRED,
        }
    }
}
