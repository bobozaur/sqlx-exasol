mod builder;
pub(crate) mod login;
mod protocol_version;
mod serializable;

use std::str::FromStr;

pub use login::{Credentials, Login};
pub use protocol_version::ProtocolVersion;
use serde::Serialize;
use sqlx_core::connection::{ConnectOptions, LogSettings};
use sqlx_core::Error as SqlxError;
use url::Url;

use crate::connection::ExaConnection;

use self::login::LoginRef;
use self::{builder::ExaConnectOptionsBuilder, serializable::SerializableConOpts};

#[derive(Debug, Clone)]
pub struct ExaConnectOptions {
    pub(crate) hosts: Vec<String>,
    pub(crate) port: u16,
    login: Login,
    schema: Option<String>,
    protocol_version: ProtocolVersion,
    fetch_size: usize,
    query_timeout: u64,
    encryption: bool,
    compression: bool,
    log_settings: LogSettings,
}

/// Connection options
impl ExaConnectOptions {
    const URL_SCHEME: &str = "exa";

    pub fn builder<'a>() -> ExaConnectOptionsBuilder<'a> {
        ExaConnectOptionsBuilder::new()
    }
}

impl FromStr for ExaConnectOptions {
    type Err = SqlxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(s).map_err(|e| SqlxError::Configuration(e.into()))?;
        Self::from_url(&url)
    }
}

impl ConnectOptions for ExaConnectOptions {
    type Connection = ExaConnection;

    fn from_url(url: &url::Url) -> Result<Self, SqlxError> {
        let scheme = url.scheme();

        if Self::URL_SCHEME != scheme {
            let msg = format!(
                "Invalid URL scheme: {scheme}, expected: {}",
                Self::URL_SCHEME
            );
            return Err(SqlxError::Configuration(msg.into()));
        }

        let mut builder = Self::builder();

        if let Some(host) = url.host_str() {
            builder.host(host);
        }

        let username = url.username();
        if !username.is_empty() {
            builder.username(username.to_owned());
        }

        if let Some(password) = url.password() {
            builder.password(password.to_owned());
        }

        if let Some(port) = url.port() {
            builder.port(port);
        }

        let opt_schema = url.path_segments().into_iter().flatten().next();

        if let Some(schema) = opt_schema {
            builder.schema(schema.to_owned());
        }

        for (name, value) in url.query_pairs() {
            match name.as_ref() {
                "access_token" => builder.access_token(value.to_string()),
                "refresh_token" => builder.refresh_token(value.to_string()),
                "protocol_version" => {
                    let protocol_version = value
                        .parse::<ProtocolVersion>()
                        .map_err(|e| SqlxError::Protocol(e.to_string()))?;
                    builder.protocol_version(protocol_version)
                }
                "fetch_size" => {
                    let fetch_size = value
                        .parse::<usize>()
                        .map_err(|e| SqlxError::Protocol(e.to_string()))?;
                    builder.fetch_size(fetch_size)
                }
                "query_timeout" => {
                    let query_timeout = value
                        .parse::<u64>()
                        .map_err(|e| SqlxError::Protocol(e.to_string()))?;
                    builder.query_timeout(query_timeout)
                }
                "encryption" => {
                    let encryption = value
                        .parse::<bool>()
                        .map_err(|e| SqlxError::Protocol(e.to_string()))?;
                    builder.encryption(encryption)
                }
                "compression" => {
                    let compression = value
                        .parse::<bool>()
                        .map_err(|e| SqlxError::Protocol(e.to_string()))?;
                    builder.compression(compression)
                }
                _ => {
                    return Err(SqlxError::Protocol(format!(
                        "Unknown connection string argument: {value}"
                    )))
                }
            };
        }

        builder.build().map_err(SqlxError::Protocol)
    }

    fn connect(&self) -> futures_util::future::BoxFuture<'_, Result<Self::Connection, SqlxError>>
    where
        Self::Connection: Sized,
    {
        Box::pin(async move {
            ExaConnection::establish(self)
                .await
                .map_err(SqlxError::Protocol)
        })
    }

    fn log_statements(mut self, level: log::LevelFilter) -> Self {
        self.log_settings.log_statements(level);
        self
    }

    fn log_slow_statements(
        mut self,
        level: log::LevelFilter,
        duration: std::time::Duration,
    ) -> Self {
        self.log_settings.log_slow_statements(level, duration);
        self
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(into = "SerializableConOpts")]
pub(crate) struct ExaConnectOptionsRef<'a> {
    pub(crate) login: LoginRef<'a>,
    pub(crate) protocol_version: ProtocolVersion,
    schema: Option<&'a str>,
    fetch_size: usize,
    query_timeout: u64,
    encryption: bool,
    compression: bool,
}

impl<'a> From<&'a ExaConnectOptions> for ExaConnectOptionsRef<'a> {
    fn from(value: &'a ExaConnectOptions) -> Self {
        Self {
            login: LoginRef::from(&value.login),
            protocol_version: value.protocol_version,
            schema: value.schema.as_deref(),
            fetch_size: value.fetch_size,
            query_timeout: value.query_timeout,
            encryption: value.encryption,
            compression: value.compression,
        }
    }
}
