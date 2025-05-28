mod builder;
mod error;
mod login;
mod protocol_version;
mod serializable;
mod ssl_mode;

use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, str::FromStr};

pub use builder::ExaConnectOptionsBuilder;
use error::ExaConfigError;
pub use login::{Credentials, CredentialsRef, Login, LoginRef};
pub use protocol_version::ProtocolVersion;
use serde::Serialize;
use serializable::SerializableConOpts;
use sqlx_core::{
    connection::{ConnectOptions, LogSettings},
    net::tls::CertificateInput,
    Error as SqlxError,
};
pub use ssl_mode::ExaSslMode;
use tracing::log;
use url::Url;

use crate::connection::ExaConnection;

pub const URL_SCHEME: &str = "exa";

pub const DEFAULT_FETCH_SIZE: usize = 5 * 1024 * 1024;
pub const DEFAULT_PORT: u16 = 8563;
pub const DEFAULT_CACHE_CAPACITY: NonZeroUsize = match NonZeroUsize::new(100) {
    Some(v) => v,
    None => unreachable!(),
};

pub const PARAM_ACCESS_TOKEN: &str = "access-token";
pub const PARAM_REFRESH_TOKEN: &str = "refresh-token";
pub const PARAM_PROTOCOL_VERSION: &str = "protocol-version";
pub const PARAM_SSL_MODE: &str = "ssl-mode";
pub const PARAM_SSL_CA: &str = "ssl-ca";
pub const PARAM_SSL_CERT: &str = "ssl-cert";
pub const PARAM_SSL_KEY: &str = "ssl-key";
pub const PARAM_CACHE_CAP: &str = "statement-cache-capacity";
pub const PARAM_FETCH_SIZE: &str = "fetch-size";
pub const PARAM_QUERY_TIMEOUT: &str = "query-timeout";
pub const PARAM_COMPRESSION: &str = "compression";
pub const PARAM_FEEDBACK_INTERVAL: &str = "feedback-interval";

/// Options for connecting to the Exasol database.
///
/// While generally automatically created through a connection string,
/// [`ExaConnectOptions::builder()`] can be used to get a [`ExaConnectOptionsBuilder`].
#[derive(Debug, Clone)]
pub struct ExaConnectOptions {
    pub(crate) hosts_details: Vec<(String, Vec<SocketAddr>)>,
    pub(crate) port: u16,
    pub(crate) ssl_mode: ExaSslMode,
    pub(crate) ssl_ca: Option<CertificateInput>,
    pub(crate) ssl_client_cert: Option<CertificateInput>,
    pub(crate) ssl_client_key: Option<CertificateInput>,
    pub(crate) statement_cache_capacity: NonZeroUsize,
    pub(crate) schema: Option<String>,
    pub(crate) compression: bool,
    login: Login,
    protocol_version: ProtocolVersion,
    fetch_size: usize,
    query_timeout: u64,
    feedback_interval: u8,
    log_settings: LogSettings,
}

impl ExaConnectOptions {
    #[must_use]
    pub fn builder() -> ExaConnectOptionsBuilder {
        ExaConnectOptionsBuilder::default()
    }
}

impl FromStr for ExaConnectOptions {
    type Err = SqlxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(s)
            .map_err(From::from)
            .map_err(SqlxError::Configuration)?;
        Self::from_url(&url)
    }
}

impl ConnectOptions for ExaConnectOptions {
    type Connection = ExaConnection;

    fn from_url(url: &Url) -> Result<Self, SqlxError> {
        let scheme = url.scheme();

        if URL_SCHEME != scheme {
            return Err(ExaConfigError::InvalidUrlScheme(scheme.to_owned()).into());
        }

        let mut builder = Self::builder();

        if let Some(host) = url.host_str() {
            builder = builder.host(host.to_owned());
        }

        let username = url.username();
        if !username.is_empty() {
            builder = builder.username(username.to_owned());
        }

        if let Some(password) = url.password() {
            builder = builder.password(password.to_owned());
        }

        if let Some(port) = url.port() {
            builder = builder.port(port);
        }

        let opt_schema = url.path_segments().into_iter().flatten().next();

        if let Some(schema) = opt_schema {
            builder = builder.schema(schema.to_owned());
        }

        for (name, value) in url.query_pairs() {
            match name.as_ref() {
                PARAM_ACCESS_TOKEN => builder = builder.access_token(value.to_string()),

                PARAM_REFRESH_TOKEN => builder = builder.refresh_token(value.to_string()),

                PARAM_PROTOCOL_VERSION => {
                    let protocol_version = value.parse::<ProtocolVersion>()?;
                    builder = builder.protocol_version(protocol_version);
                }

                PARAM_SSL_MODE => {
                    let ssl_mode = value.parse::<ExaSslMode>()?;
                    builder = builder.ssl_mode(ssl_mode);
                }

                PARAM_SSL_CA => {
                    let ssl_ca = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder = builder.ssl_ca(ssl_ca);
                }

                PARAM_SSL_CERT => {
                    let ssl_cert = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder = builder.ssl_client_cert(ssl_cert);
                }

                PARAM_SSL_KEY => {
                    let ssl_key = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder = builder.ssl_client_key(ssl_key);
                }

                PARAM_CACHE_CAP => {
                    let capacity = value
                        .parse::<NonZeroUsize>()
                        .map_err(|_| ExaConfigError::InvalidParameter(PARAM_CACHE_CAP))?;
                    builder = builder.statement_cache_capacity(capacity);
                }

                PARAM_FETCH_SIZE => {
                    let fetch_size = value
                        .parse::<usize>()
                        .map_err(|_| ExaConfigError::InvalidParameter(PARAM_FETCH_SIZE))?;
                    builder = builder.fetch_size(fetch_size);
                }

                PARAM_QUERY_TIMEOUT => {
                    let query_timeout = value
                        .parse::<u64>()
                        .map_err(|_| ExaConfigError::InvalidParameter(PARAM_QUERY_TIMEOUT))?;
                    builder = builder.query_timeout(query_timeout);
                }

                PARAM_COMPRESSION => {
                    let compression = value
                        .parse::<bool>()
                        .map_err(|_| ExaConfigError::InvalidParameter(PARAM_COMPRESSION))?;
                    builder = builder.compression(compression);
                }

                PARAM_FEEDBACK_INTERVAL => {
                    let feedback_interval = value
                        .parse::<u8>()
                        .map_err(|_| ExaConfigError::InvalidParameter(PARAM_FEEDBACK_INTERVAL))?;
                    builder = builder.feedback_interval(feedback_interval);
                }

                _ => {
                    return Err(SqlxError::Protocol(format!(
                        "Unknown connection string parameter: {value}"
                    )))
                }
            };
        }

        builder.build()
    }

    fn connect(&self) -> futures_util::future::BoxFuture<'_, Result<Self::Connection, SqlxError>>
    where
        Self::Connection: Sized,
    {
        Box::pin(ExaConnection::establish(self))
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

/// Serialization helper that borrows as much data as possible.
/// This type cannot be [`Copy`] because of [`LoginRef`].
//
// We use a single set of connection options to create a connections pool,
// yet each connection's password is encrypted individually with a
// public key specific to that connection, so it will be different and is
// stored as such.
#[derive(Debug, Clone, Serialize)]
#[serde(into = "SerializableConOpts<'_>")]
pub struct ExaConnectOptionsRef<'a> {
    pub login: LoginRef<'a>,
    pub protocol_version: ProtocolVersion,
    pub schema: Option<&'a str>,
    pub fetch_size: usize,
    pub query_timeout: u64,
    pub compression: bool,
    pub feedback_interval: u8,
    pub statement_cache_capacity: NonZeroUsize,
}

impl<'a> From<&'a ExaConnectOptions> for ExaConnectOptionsRef<'a> {
    fn from(value: &'a ExaConnectOptions) -> Self {
        Self {
            login: LoginRef::from(&value.login),
            protocol_version: value.protocol_version,
            schema: value.schema.as_deref(),
            fetch_size: value.fetch_size,
            query_timeout: value.query_timeout,
            compression: value.compression,
            feedback_interval: value.feedback_interval,
            statement_cache_capacity: value.statement_cache_capacity,
        }
    }
}

impl<'a> TryFrom<&'a ExaConnectOptionsRef<'a>> for String {
    type Error = SqlxError;

    fn try_from(value: &'a ExaConnectOptionsRef<'a>) -> Result<Self, Self::Error> {
        serde_json::to_string(value).map_err(|e| SqlxError::Protocol(e.to_string()))
    }
}

/// Helper containing TLS related options.
#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_field_names)]
pub struct ExaTlsOptionsRef<'a> {
    pub ssl_mode: ExaSslMode,
    pub ssl_ca: Option<&'a CertificateInput>,
    pub ssl_client_cert: Option<&'a CertificateInput>,
    pub ssl_client_key: Option<&'a CertificateInput>,
}

impl<'a> From<&'a ExaConnectOptions> for ExaTlsOptionsRef<'a> {
    fn from(value: &'a ExaConnectOptions) -> Self {
        ExaTlsOptionsRef {
            ssl_mode: value.ssl_mode,
            ssl_ca: value.ssl_ca.as_ref(),
            ssl_client_cert: value.ssl_client_cert.as_ref(),
            ssl_client_key: value.ssl_client_key.as_ref(),
        }
    }
}
