use std::str::FromStr;

use super::{error::ExaConfigError, PARAM_SSL_MODE};

/// Options for controlling the desired security state of the connection to the Exasol server.
///
/// It is used by the `ssl_mode` method of [`crate::options::builder::ExaConnectOptionsBuilder`].
#[derive(Debug, Clone, Copy, Default)]
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

impl FromStr for ExaSslMode {
    type Err = ExaConfigError;

    fn from_str(s: &str) -> Result<Self, ExaConfigError> {
        Ok(match &*s.to_ascii_lowercase() {
            "disabled" => ExaSslMode::Disabled,
            "preferred" => ExaSslMode::Preferred,
            "required" => ExaSslMode::Required,
            "verify_ca" => ExaSslMode::VerifyCa,
            "verify_identity" => ExaSslMode::VerifyIdentity,

            _ => {
                return Err(ExaConfigError::InvalidParameter(PARAM_SSL_MODE));
            }
        })
    }
}
