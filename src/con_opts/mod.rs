mod builder;
mod credentials;
mod protocol_version;
mod serializable;

use std::{net::SocketAddr, str::FromStr};

pub use credentials::{Credentials, LoginKind};
pub use protocol_version::ProtocolVersion;
use serde::Serialize;
use sqlx_core::connection::ConnectOptions;
use sqlx_core::Error as SqlxError;

use crate::connection::ExaConnection;

use self::{builder::ConOptsBuilder, serializable::SerializableConOpts};

/// Connection options for [Connection](crate::Connection)
/// The DSN may or may not contain a port - if it does not,
/// the port field in this struct is used as a fallback.
///
/// Default is implemented for [ConOpts] so that most fields have fallback values.
/// DSN, user, and password fields are practically mandatory,
/// as they otherwise default to an empty string or JSON null.
///
/// # Defaults
///
/// schema: None
/// port: 8563
/// protocol_version: ProtocolVersion::V3
/// fetch_size: 5 * 1024 * 1024
/// query_timeout: 0
/// use_encryption: *if encryption features are enabled true, else false*
/// use_compression: false
/// lowercase_columns: true
/// autocommit: true
///
/// ```
///  use exasol::{ConOpts, LoginKind, Credentials};
///
///  let mut opts = ConOpts::new(); // calls default() under the hood
///  opts.set_dsn("test_dsn");
///  opts.set_login_kind(LoginKind::Credentials(Credentials::new("test_user", "test_password")));
///  opts.set_schema(Some("test_schema"));
///  opts.set_autocommit(false);
/// ```
#[derive(Debug, Clone)]
pub struct ExaConnectOptions {
    addresses: Vec<SocketAddr>,
    fingerprint: Option<String>,
    login_kind: LoginKind,
    schema: Option<String>,
    protocol_version: ProtocolVersion,
    client_name: String,
    client_version: String,
    client_os: String,
    fetch_size: usize,
    query_timeout: u64,
    use_encryption: bool,
    use_compression: bool,
    lowercase_columns: bool,
    autocommit: bool,
}

impl ExaConnectOptions {
    const INSECURE_PREFIX: &str = "ws";
    const SECURE_PREFIX: &str = "wss";
}

/// Connection options
impl ExaConnectOptions {
    /// Creates a default implementation of [ConOpts]
    pub fn builder() -> ConOptsBuilder<false, false> {
        ConOptsBuilder::new()
    }

    /// Convenience method for determining the websocket type.
    #[inline]
    pub(crate) fn ws_prefix(&self) -> &str {
        match self.use_encryption {
            false => Self::INSECURE_PREFIX,
            true => Self::SECURE_PREFIX,
        }
    }

    #[inline]
    pub(crate) fn compression(&self) -> bool {
        self.use_compression
    }

    #[inline]
    pub(crate) fn login_kind_mut(&mut self) -> &mut LoginKind {
        &mut self.login_kind
    }
}

impl Serialize for ExaConnectOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SerializableConOpts::from(self).serialize(serializer)
    }
}

impl FromStr for ExaConnectOptions {
    type Err = SqlxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

impl ConnectOptions for ExaConnectOptions {
    type Connection = ExaConnection;

    fn from_url(url: &url::Url) -> Result<Self, SqlxError> {
        todo!()
    }

    fn connect(&self) -> futures_util::future::BoxFuture<'_, Result<Self::Connection, SqlxError>>
    where
        Self::Connection: Sized,
    {
        todo!()
    }

    fn log_statements(self, level: log::LevelFilter) -> Self {
        todo!()
    }

    fn log_slow_statements(self, level: log::LevelFilter, duration: std::time::Duration) -> Self {
        todo!()
    }
}
