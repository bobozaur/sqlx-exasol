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
