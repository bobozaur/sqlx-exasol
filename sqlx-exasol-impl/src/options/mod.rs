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
};
pub use ssl_mode::ExaSslMode;
use tracing::log;
use url::Url;

use crate::{
    connection::{
        websocket::request::{ExaLoginRequest, LoginRef},
        ExaConnection,
    },
    responses::ExaRwAttributes,
    SqlxError, SqlxResult,
};

const URL_SCHEME: &str = "exa";

const DEFAULT_FETCH_SIZE: usize = 5 * 1024 * 1024;
const DEFAULT_PORT: u16 = 8563;
const DEFAULT_CACHE_CAPACITY: NonZeroUsize = match NonZeroUsize::new(100) {
    Some(v) => v,
    None => unreachable!(),
};

const ACCESS_TOKEN: &str = "access-token";
const REFRESH_TOKEN: &str = "refresh-token";
const PROTOCOL_VERSION: &str = "protocol-version";
const SSL_MODE: &str = "ssl-mode";
const SSL_CA: &str = "ssl-ca";
const SSL_CERT: &str = "ssl-cert";
const SSL_KEY: &str = "ssl-key";
const STATEMENT_CACHE_CAPACITY: &str = "statement-cache-capacity";
const FETCH_SIZE: &str = "fetch-size";
const QUERY_TIMEOUT: &str = "query-timeout";
const COMPRESSION: &str = "compression";
const FEEDBACK_INTERVAL: &str = "feedback-interval";

/// Options for connecting to the Exasol database. Implementor of [`ConnectOptions`].
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
    pub(crate) log_settings: LogSettings,
    hostname: String,
    login: Login,
    protocol_version: ProtocolVersion,
    fetch_size: usize,
    query_timeout: u64,
    feedback_interval: u64,
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

    fn from_url(url: &Url) -> SqlxResult<Self> {
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
                ACCESS_TOKEN => builder = builder.access_token(value.to_string()),

                REFRESH_TOKEN => builder = builder.refresh_token(value.to_string()),

                PROTOCOL_VERSION => {
                    let protocol_version = value.parse::<ProtocolVersion>()?;
                    builder = builder.protocol_version(protocol_version);
                }

                SSL_MODE => {
                    let ssl_mode = value.parse::<ExaSslMode>()?;
                    builder = builder.ssl_mode(ssl_mode);
                }

                SSL_CA => {
                    let ssl_ca = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder = builder.ssl_ca(ssl_ca);
                }

                SSL_CERT => {
                    let ssl_cert = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder = builder.ssl_client_cert(ssl_cert);
                }

                SSL_KEY => {
                    let ssl_key = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder = builder.ssl_client_key(ssl_key);
                }

                STATEMENT_CACHE_CAPACITY => {
                    let capacity = value
                        .parse::<NonZeroUsize>()
                        .map_err(|_| ExaConfigError::InvalidParameter(STATEMENT_CACHE_CAPACITY))?;
                    builder = builder.statement_cache_capacity(capacity);
                }

                FETCH_SIZE => {
                    let fetch_size = value
                        .parse::<usize>()
                        .map_err(|_| ExaConfigError::InvalidParameter(FETCH_SIZE))?;
                    builder = builder.fetch_size(fetch_size);
                }

                QUERY_TIMEOUT => {
                    let query_timeout = value
                        .parse::<u64>()
                        .map_err(|_| ExaConfigError::InvalidParameter(QUERY_TIMEOUT))?;
                    builder = builder.query_timeout(query_timeout);
                }

                COMPRESSION => {
                    let compression = value
                        .parse::<bool>()
                        .map_err(|_| ExaConfigError::InvalidParameter(COMPRESSION))?;
                    builder = builder.compression(compression);
                }

                FEEDBACK_INTERVAL => {
                    let feedback_interval = value
                        .parse::<u64>()
                        .map_err(|_| ExaConfigError::InvalidParameter(FEEDBACK_INTERVAL))?;
                    builder = builder.feedback_interval(feedback_interval);
                }

                _ => {
                    return Err(SqlxError::Protocol(format!(
                        "Unknown connection string parameter: {value}"
                    )))
                }
            }
        }

        builder.build()
    }

    fn to_url_lossy(&self) -> Url {
        let mut url = Url::parse(&format!("{URL_SCHEME}://{}:{}", self.hostname, self.port))
            .expect("generated URL must be correct");

        if let Some(schema) = &self.schema {
            url.set_path(schema);
        }

        match &self.login {
            Login::Credentials { username, password } => {
                url.set_username(username).ok();
                url.set_password(Some(password)).ok();
            }
            Login::AccessToken { access_token } => {
                url.query_pairs_mut()
                    .append_pair(ACCESS_TOKEN, access_token);
            }
            Login::RefreshToken { refresh_token } => {
                url.query_pairs_mut()
                    .append_pair(REFRESH_TOKEN, refresh_token);
            }
        }

        url.query_pairs_mut()
            .append_pair(PROTOCOL_VERSION, &self.protocol_version.to_string());

        url.query_pairs_mut()
            .append_pair(SSL_MODE, self.ssl_mode.as_ref());

        if let Some(ssl_ca) = &self.ssl_ca {
            url.query_pairs_mut()
                .append_pair(SSL_CA, &ssl_ca.to_string());
        }

        if let Some(ssl_cert) = &self.ssl_client_cert {
            url.query_pairs_mut()
                .append_pair(SSL_CERT, &ssl_cert.to_string());
        }

        if let Some(ssl_key) = &self.ssl_client_key {
            url.query_pairs_mut()
                .append_pair(SSL_KEY, &ssl_key.to_string());
        }

        url.query_pairs_mut().append_pair(
            STATEMENT_CACHE_CAPACITY,
            &self.statement_cache_capacity.to_string(),
        );

        url.query_pairs_mut()
            .append_pair(FETCH_SIZE, &self.fetch_size.to_string());

        url.query_pairs_mut()
            .append_pair(QUERY_TIMEOUT, &self.query_timeout.to_string());

        url.query_pairs_mut()
            .append_pair(COMPRESSION, &self.compression.to_string());

        url.query_pairs_mut()
            .append_pair(FEEDBACK_INTERVAL, &self.feedback_interval.to_string());

        url
    }

    fn connect(&self) -> futures_util::future::BoxFuture<'_, SqlxResult<Self::Connection>>
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

        let attributes = ExaRwAttributes::new(
            value.schema.as_deref().map(Cow::Borrowed),
            value.feedback_interval,
            value.query_timeout,
        );

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
