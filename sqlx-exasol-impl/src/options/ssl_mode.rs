use std::str::FromStr;

use super::{error::ExaConfigError, SSL_MODE};

/// Options for controlling the desired security state of the connection to the Exasol server.
///
/// It is used by [`crate::options::builder::ExaConnectOptionsBuilder::ssl_mode`].
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum ExaSslMode {
    /// Establish an unencrypted connection.
    Disabled,

    /// Establish an encrypted connection if the server supports encrypted connections, falling
    /// back to an unencrypted connection if an encrypted connection cannot be established.
    ///
    /// This is the default if `ssl_mode` is not specified.
    #[default]
    Preferred,

    /// Establish an encrypted connection if the server supports encrypted connections.
    /// The connection attempt fails if an encrypted connection cannot be established.
    Required,

    /// Like `Required`, but additionally verify the server Certificate Authority (CA)
    /// certificate against the configured CA certificates. The connection attempt fails
    /// if no valid matching CA certificates are found.
    VerifyCa,

    /// Like `VerifyCa`, but additionally perform host name identity verification by
    /// checking the host name the client uses for connecting to the server against the
    /// identity in the certificate that the server sends to the client.
    VerifyIdentity,
}

impl ExaSslMode {
    const DISABLED: &str = "disabled";
    const PREFERRED: &str = "preferred";
    const REQUIRED: &str = "required";
    const VERIFY_CA: &str = "verify_ca";
    const VERIFY_IDENTITY: &str = "verify_identity";
}

impl FromStr for ExaSslMode {
    type Err = ExaConfigError;

    fn from_str(s: &str) -> Result<Self, ExaConfigError> {
        Ok(match &*s.to_ascii_lowercase() {
            Self::DISABLED => ExaSslMode::Disabled,
            Self::PREFERRED => ExaSslMode::Preferred,
            Self::REQUIRED => ExaSslMode::Required,
            Self::VERIFY_CA => ExaSslMode::VerifyCa,
            Self::VERIFY_IDENTITY => ExaSslMode::VerifyIdentity,
            _ => Err(ExaConfigError::InvalidParameter(SSL_MODE))?,
        })
    }
}

impl AsRef<str> for ExaSslMode {
    fn as_ref(&self) -> &str {
        match self {
            ExaSslMode::Disabled => Self::DISABLED,
            ExaSslMode::Preferred => Self::PREFERRED,
            ExaSslMode::Required => Self::REQUIRED,
            ExaSslMode::VerifyCa => Self::VERIFY_CA,
            ExaSslMode::VerifyIdentity => Self::VERIFY_IDENTITY,
        }
    }
}
