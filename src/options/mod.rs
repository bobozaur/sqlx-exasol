mod builder;
mod error;
mod protocol_version;
mod ssl_mode;

use std::{borrow::Cow, net::SocketAddr, num::NonZeroUsize, path::PathBuf, str::FromStr};

pub use builder::ExaConnectOptionsBuilder;
use error::ExaConfigError;
pub use protocol_version::ProtocolVersion;
use sqlx_core::{
    connection::{ConnectOptions, LogSettings},
    net::tls::CertificateInput,
    Error as SqlxError,
};
pub use ssl_mode::ExaSslMode;
use tracing::log;
use url::Url;

use crate::connection::{
    websocket::request::{ExaLoginRequest, LoginAttrs, LoginRef},
    ExaConnection,
};

const URL_SCHEME: &str = "exa";

const DEFAULT_FETCH_SIZE: usize = 5 * 1024 * 1024;
const DEFAULT_PORT: u16 = 8563;
const DEFAULT_CACHE_CAPACITY: NonZeroUsize = match NonZeroUsize::new(100) {
    Some(v) => v,
    None => unreachable!(),
};

const PARAM_ACCESS_TOKEN: &str = "access-token";
const PARAM_REFRESH_TOKEN: &str = "refresh-token";
const PARAM_PROTOCOL_VERSION: &str = "protocol-version";
const PARAM_SSL_MODE: &str = "ssl-mode";
const PARAM_SSL_CA: &str = "ssl-ca";
const PARAM_SSL_CERT: &str = "ssl-cert";
const PARAM_SSL_KEY: &str = "ssl-key";
const PARAM_CACHE_CAP: &str = "statement-cache-capacity";
const PARAM_FETCH_SIZE: &str = "fetch-size";
const PARAM_QUERY_TIMEOUT: &str = "query-timeout";
const PARAM_COMPRESSION: &str = "compression";
const PARAM_FEEDBACK_INTERVAL: &str = "feedback-interval";

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

impl<'a> From<&'a ExaConnectOptions> for ExaLoginRequest<'a> {
    fn from(value: &'a ExaConnectOptions) -> Self {
        let crate_version = option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN");

        let attributes = LoginAttrs {
            current_schema: value.schema.as_deref(),
            query_timeout: value.query_timeout,
            autocommit: true,
            feedback_interval: value.feedback_interval,
        };

        Self {
            protocol_version: value.protocol_version,
            fetch_size: value.fetch_size,
            statement_cache_capacity: value.statement_cache_capacity,
            login: (&value.login).into(),
            use_compression: value.compression,
            client_name: "sqlx-exasol",
            client_version: crate_version,
            client_os: std::env::consts::OS,
            client_runtime: "RUST",
            attributes,
        }
    }
}

/// Enum representing the possible ways of authenticating a connection.
/// The variant chosen dictates which login process is called.
#[derive(Clone, Debug)]
pub enum Login {
    Credentials { username: String, password: String },
    AccessToken { access_token: String },
    RefreshToken { refresh_token: String },
}

impl<'a> From<&'a Login> for LoginRef<'a> {
    fn from(value: &'a Login) -> Self {
        match value {
            Login::Credentials { username, password } => LoginRef::Credentials {
                username,
                password: Cow::Borrowed(password),
            },
            Login::AccessToken { access_token } => LoginRef::AccessToken { access_token },
            Login::RefreshToken { refresh_token } => LoginRef::RefreshToken { refresh_token },
        }
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
