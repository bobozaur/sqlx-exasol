use sqlx_core::Error as SqlxError;
use std::str::FromStr;

/// Options for controlling the desired security state of the connection to the MySQL server.
///
/// It is used by the [`ssl_mode`](super::MySqlConnectOptions::ssl_mode) method.
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
    type Err = SqlxError;

    fn from_str(s: &str) -> Result<Self, SqlxError> {
        Ok(match &*s.to_ascii_lowercase() {
            "disabled" => ExaSslMode::Disabled,
            "preferred" => ExaSslMode::Preferred,
            "required" => ExaSslMode::Required,
            "verify_ca" => ExaSslMode::VerifyCa,
            "verify_identity" => ExaSslMode::VerifyIdentity,

            _ => {
                return Err(SqlxError::Configuration(
                    format!("unknown value {:?} for `ssl_mode`", s).into(),
                ));
            }
        })
    }
}
