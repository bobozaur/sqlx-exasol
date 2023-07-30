mod builder;
pub(crate) mod login;
mod protocol_version;
mod serializable;
pub(crate) mod ssl_mode;

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;

pub use login::{Credentials, Login};
pub use protocol_version::ProtocolVersion;
use serde::Serialize;
use sqlx_core::connection::{ConnectOptions, LogSettings};
use sqlx_core::net::tls::CertificateInput;
use sqlx_core::Error as SqlxError;
use url::Url;

use crate::connection::ExaConnection;

use self::login::LoginRef;
use self::ssl_mode::ExaSslMode;
use self::{builder::ExaConnectOptionsBuilder, serializable::SerializableConOpts};

pub(crate) const DEFAULT_FETCH_SIZE: usize = 5 * 1024 * 1024;
pub(crate) const DEFAULT_PORT: u16 = 8563;
pub(crate) const DEFAULT_CACHE_CAPACITY: usize = 100;

#[derive(Debug, Clone)]
pub struct ExaConnectOptions {
    pub(crate) hosts: Vec<String>,
    pub(crate) port: u16,
    pub(crate) ssl_mode: ExaSslMode,
    pub(crate) ssl_ca: Option<CertificateInput>,
    pub(crate) ssl_client_cert: Option<CertificateInput>,
    pub(crate) ssl_client_key: Option<CertificateInput>,
    pub(crate) statement_cache_capacity: NonZeroUsize,
    pub(crate) schema: Option<String>,
    login: Login,
    protocol_version: ProtocolVersion,
    fetch_size: usize,
    query_timeout: u64,
    compression: bool,
    feedback_interval: u8,
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

                "ssl-mode" => {
                    let ssl_mode = value
                        .parse::<ExaSslMode>()
                        .map_err(|e| SqlxError::Protocol(e.to_string()))?;
                    builder.ssl_mode(ssl_mode)
                }

                "ssl-ca" => {
                    let ssl_ca = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder.ssl_ca(ssl_ca)
                }

                "ssl-cert" => {
                    let ssl_cert = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder.ssl_client_cert(ssl_cert)
                }

                "ssl-key" => {
                    let ssl_key = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder.ssl_client_key(ssl_key)
                }

                "statement-cache-capacity" => {
                    let capacity = value
                        .parse::<NonZeroUsize>()
                        .map_err(|e| SqlxError::Protocol(e.to_string()))?;
                    builder.statement_cache_capacity(capacity)
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

                "compression" => {
                    let compression = value
                        .parse::<bool>()
                        .map_err(|e| SqlxError::Protocol(e.to_string()))?;
                    builder.compression(compression)
                }

                "feedback_interval" => {
                    let feedback_interval = value
                        .parse::<u8>()
                        .map_err(|e| SqlxError::Protocol(e.to_string()))?;
                    builder.feedback_interval(feedback_interval)
                }

                _ => {
                    return Err(SqlxError::Protocol(format!(
                        "Unknown connection string parameter: {value}"
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
    pub(crate) ssl_mode: ExaSslMode,
    pub(crate) ssl_ca: Option<&'a CertificateInput>,
    pub(crate) ssl_client_cert: Option<&'a CertificateInput>,
    pub(crate) ssl_client_key: Option<&'a CertificateInput>,
    pub(crate) schema: Option<&'a str>,
    pub(crate) fetch_size: usize,
    pub(crate) query_timeout: u64,
    pub(crate) compression: bool,
    pub(crate) feedback_interval: u8,
    pub(crate) statement_cache_capacity: NonZeroUsize,
}

impl<'a> From<&'a ExaConnectOptions> for ExaConnectOptionsRef<'a> {
    fn from(value: &'a ExaConnectOptions) -> Self {
        Self {
            login: LoginRef::from(&value.login),
            protocol_version: value.protocol_version,
            ssl_mode: value.ssl_mode,
            ssl_ca: value.ssl_ca.as_ref(),
            ssl_client_cert: value.ssl_client_cert.as_ref(),
            ssl_client_key: value.ssl_client_key.as_ref(),
            schema: value.schema.as_deref(),
            fetch_size: value.fetch_size,
            query_timeout: value.query_timeout,
            compression: value.compression,
            feedback_interval: value.feedback_interval,
            statement_cache_capacity: value.statement_cache_capacity,
        }
    }
}
