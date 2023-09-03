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

pub(crate) const URL_SCHEME: &str = "exa";

pub(crate) const DEFAULT_FETCH_SIZE: usize = 5 * 1024 * 1024;
pub(crate) const DEFAULT_PORT: u16 = 8563;
pub(crate) const DEFAULT_CACHE_CAPACITY: usize = 100;

pub(crate) const PARAM_ACCESS_TOKEN: &str = "access-token";
pub(crate) const PARAM_REFRESH_TOKEN: &str = "refresh-token";
pub(crate) const PARAM_PROTOCOL_VERSION: &str = "protocol-version";
pub(crate) const PARAM_SSL_MODE: &str = "ssl-mode";
pub(crate) const PARAM_SSL_CA: &str = "ssl-ca";
pub(crate) const PARAM_SSL_CERT: &str = "ssl-cert";
pub(crate) const PARAM_SSL_KEY: &str = "ssl-key";
pub(crate) const PARAM_CACHE_CAP: &str = "statement-cache-capacity";
pub(crate) const PARAM_FETCH_SIZE: &str = "fetch-size";
pub(crate) const PARAM_QUERY_TIMEOUT: &str = "query-timeout";
pub(crate) const PARAM_COMPRESSION: &str = "compression";
pub(crate) const PARAM_FEEDBACK_INTERVAL: &str = "feedback-interval";

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
    login: Login,
    protocol_version: ProtocolVersion,
    fetch_size: usize,
    query_timeout: u64,
    compression: bool,
    feedback_interval: u8,
    log_settings: LogSettings,
}

impl ExaConnectOptions {
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
            builder.host(host.to_owned());
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
                PARAM_ACCESS_TOKEN => builder.access_token(value.to_string()),

                PARAM_REFRESH_TOKEN => builder.refresh_token(value.to_string()),

                PARAM_PROTOCOL_VERSION => {
                    let protocol_version = value.parse::<ProtocolVersion>()?;
                    builder.protocol_version(protocol_version)
                }

                PARAM_SSL_MODE => {
                    let ssl_mode = value.parse::<ExaSslMode>()?;
                    builder.ssl_mode(ssl_mode)
                }

                PARAM_SSL_CA => {
                    let ssl_ca = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder.ssl_ca(ssl_ca)
                }

                PARAM_SSL_CERT => {
                    let ssl_cert = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder.ssl_client_cert(ssl_cert)
                }

                PARAM_SSL_KEY => {
                    let ssl_key = CertificateInput::File(PathBuf::from(value.to_string()));
                    builder.ssl_client_key(ssl_key)
                }

                PARAM_CACHE_CAP => {
                    let capacity = value
                        .parse::<NonZeroUsize>()
                        .map_err(|_| ExaConfigError::InvalidParameter(PARAM_CACHE_CAP))?;
                    builder.statement_cache_capacity(capacity)
                }

                PARAM_FETCH_SIZE => {
                    let fetch_size = value
                        .parse::<usize>()
                        .map_err(|_| ExaConfigError::InvalidParameter(PARAM_FETCH_SIZE))?;
                    builder.fetch_size(fetch_size)
                }

                PARAM_QUERY_TIMEOUT => {
                    let query_timeout = value
                        .parse::<u64>()
                        .map_err(|_| ExaConfigError::InvalidParameter(PARAM_QUERY_TIMEOUT))?;
                    builder.query_timeout(query_timeout)
                }

                PARAM_COMPRESSION => {
                    let compression = value
                        .parse::<bool>()
                        .map_err(|_| ExaConfigError::InvalidParameter(PARAM_COMPRESSION))?;
                    builder.compression(compression)
                }

                PARAM_FEEDBACK_INTERVAL => {
                    let feedback_interval = value
                        .parse::<u8>()
                        .map_err(|_| ExaConfigError::InvalidParameter(PARAM_FEEDBACK_INTERVAL))?;
                    builder.feedback_interval(feedback_interval)
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
        Box::pin(async move { ExaConnection::establish(self).await })
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
#[derive(Debug, Clone, Serialize)]
#[serde(into = "SerializableConOpts<'_>")]
pub(crate) struct ExaConnectOptionsRef<'a> {
    pub(crate) login: LoginRef<'a>,
    pub(crate) protocol_version: ProtocolVersion,
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
            schema: value.schema.as_deref(),
            fetch_size: value.fetch_size,
            query_timeout: value.query_timeout,
            compression: value.compression,
            feedback_interval: value.feedback_interval,
            statement_cache_capacity: value.statement_cache_capacity,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExaTlsOptionsRef<'a> {
    pub(crate) ssl_mode: ExaSslMode,
    pub(crate) ssl_ca: Option<&'a CertificateInput>,
    pub(crate) ssl_client_cert: Option<&'a CertificateInput>,
    pub(crate) ssl_client_key: Option<&'a CertificateInput>,
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

#[cfg(test)]
#[cfg(feature = "migrate")]
#[allow(unused_imports)]
mod tests {
    use sqlx::Executor;
    use sqlx_core::{error::BoxDynError, pool::PoolOptions};

    use crate::{ExaConnectOptions, Exasol};

    #[cfg(feature = "compression")]
    #[ignore]
    #[sqlx::test]
    async fn test_compression_works(
        pool_opts: PoolOptions<Exasol>,
        mut exa_opts: ExaConnectOptions,
    ) -> Result<(), BoxDynError> {
        exa_opts.compression = true;

        let pool = pool_opts.connect_with(exa_opts).await?;
        let mut con = pool.acquire().await?;
        let schema = "TEST_SWITCH_SCHEMA";

        con.execute(format!("CREATE SCHEMA IF NOT EXISTS {schema};").as_str())
            .await?;

        let new_schema: String = sqlx::query_scalar("SELECT CURRENT_SCHEMA")
            .fetch_one(&mut *con)
            .await?;

        con.execute(format!("DROP SCHEMA IF EXISTS {schema} CASCADE;").as_str())
            .await?;

        assert_eq!(schema, new_schema);

        Ok(())
    }

    #[cfg(not(feature = "compression"))]
    #[sqlx::test]
    async fn test_compression_no_flag(
        pool_opts: PoolOptions<Exasol>,
        mut exa_opts: ExaConnectOptions,
    ) {
        exa_opts.compression = true;
        assert!(pool_opts.connect_with(exa_opts).await.is_err());
    }
}
