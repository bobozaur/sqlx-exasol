mod builder;
mod compression;
mod error;
mod protocol_version;
mod ssl_mode;

use std::{borrow::Cow, net::SocketAddr, num::NonZeroUsize, path::PathBuf, str::FromStr};

pub use builder::ExaConnectOptionsBuilder;
pub use compression::ExaCompressionMode;
use error::ExaConfigError;
pub use protocol_version::ProtocolVersion;
use sqlx_core::{
    connection::{ConnectOptions, LogSettings},
    net::tls::CertificateInput,
    percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC},
};
pub use ssl_mode::ExaSslMode;
use tracing::log;
use url::Url;

use crate::{
    connection::{
        websocket::request::{ExaLoginRequest, LoginRef},
        ExaConnection,
    },
    error::ExaProtocolError,
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
    pub(crate) compression_mode: ExaCompressionMode,
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
            let username = percent_decode_str(username)
                .decode_utf8()
                .map_err(SqlxError::config)?;
            builder = builder.username(username.to_string());
        }

        if let Some(password) = url.password() {
            let password = percent_decode_str(password)
                .decode_utf8()
                .map_err(SqlxError::config)?;
            builder = builder.password(password.to_string());
        }

        if let Some(port) = url.port() {
            builder = builder.port(port);
        }

        let path = url.path().trim_start_matches('/');

        if !path.is_empty() {
            let db_schema = percent_decode_str(path)
                .decode_utf8()
                .map_err(SqlxError::config)?;
            builder = builder.schema(db_schema.to_string());
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
                    let compression_mode = value
                        .parse::<ExaCompressionMode>()
                        .map_err(|_| ExaConfigError::InvalidParameter(COMPRESSION))?;
                    builder = builder.compression_mode(compression_mode);
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
                let password = utf8_percent_encode(password, NON_ALPHANUMERIC).to_string();
                url.set_password(Some(&password)).ok();
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
            .append_pair(PROTOCOL_VERSION, &(self.protocol_version as u8).to_string());

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
            .append_pair(COMPRESSION, self.compression_mode.as_ref());

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

impl<'a> TryFrom<&'a ExaConnectOptions> for ExaLoginRequest<'a> {
    type Error = ExaProtocolError;

    fn try_from(value: &'a ExaConnectOptions) -> Result<Self, Self::Error> {
        let crate_version = option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN");

        let attributes = ExaRwAttributes::new(
            value.schema.as_deref().map(Cow::Borrowed),
            value.feedback_interval,
            value.query_timeout,
        );

        let compression_supported = cfg!(feature = "compression");

        let use_compression = match value.compression_mode {
            ExaCompressionMode::Disabled => false,
            ExaCompressionMode::Preferred if !compression_supported => {
                tracing::debug!("not using compression: compression support not compiled in");
                false
            }
            ExaCompressionMode::Preferred => true,
            ExaCompressionMode::Required if compression_supported => true,
            ExaCompressionMode::Required => return Err(ExaProtocolError::CompressionDisabled),
        };

        let output = Self {
            protocol_version: value.protocol_version,
            fetch_size: value.fetch_size,
            statement_cache_capacity: value.statement_cache_capacity,
            login: (&value.login).into(),
            use_compression,
            client_name: "sqlx-exasol",
            client_version: crate_version,
            client_os: std::env::consts::OS,
            client_runtime: "RUST",
            attributes,
        };

        Ok(output)
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
#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;

    #[test]
    fn test_from_url_basic() {
        let url = "exa://user:pass@localhost:8563/schema";
        let options = ExaConnectOptions::from_str(url).unwrap();

        assert_eq!(options.hostname, "localhost");
        assert_eq!(options.port, 8563);
        assert_eq!(options.schema.as_deref(), Some("schema"));

        match &options.login {
            Login::Credentials { username, password } => {
                assert_eq!(username, "user");
                assert_eq!(password, "pass");
            }
            _ => panic!("Expected credentials login"),
        }
    }

    #[test]
    fn test_from_url_with_query_params() {
        let url = "exa://localhost:8563?access-token=token123&compression=disabled&fetch-size=1024";
        let options = ExaConnectOptions::from_str(url).unwrap();

        match &options.login {
            Login::AccessToken { access_token } => {
                assert_eq!(access_token, "token123");
            }
            _ => panic!("Expected access token login"),
        }

        assert_eq!(options.compression_mode, ExaCompressionMode::Disabled);
        assert_eq!(options.fetch_size, 1024);
    }

    #[test]
    fn test_from_url_refresh_token() {
        let url = "exa://localhost:8563?refresh-token=refresh123";
        let options = ExaConnectOptions::from_str(url).unwrap();

        match &options.login {
            Login::RefreshToken { refresh_token } => {
                assert_eq!(refresh_token, "refresh123");
            }
            _ => panic!("Expected refresh token login"),
        }
    }

    #[test]
    fn test_from_url_ssl_params() {
        let url = "exa://user:p@ssw0rd@localhost:8563?ssl-mode=required&ssl-ca=/path/to/ca.crt";
        let options = ExaConnectOptions::from_str(url).unwrap();

        assert_eq!(options.ssl_mode, ExaSslMode::Required);
        assert!(options.ssl_ca.is_some());
    }

    #[test]
    fn test_from_url_numeric_params() {
        let url = "exa://user:p@ssw0rd@localhost:8563?statement-cache-capacity=50&\
                   query-timeout=30&feedback-interval=10";
        let options = ExaConnectOptions::from_str(url).unwrap();

        assert_eq!(
            options.statement_cache_capacity,
            NonZeroUsize::new(50).unwrap()
        );
        assert_eq!(options.query_timeout, 30);
        assert_eq!(options.feedback_interval, 10);
    }

    #[test]
    fn test_from_url_invalid_scheme() {
        let url = "mysql://localhost:8563";
        let result = ExaConnectOptions::from_str(url);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_url_unknown_parameter() {
        let url = "exa://localhost:8563?unknown-param=value";
        let result = ExaConnectOptions::from_str(url);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_url_lossy_credentials() {
        let options = ExaConnectOptions::builder()
            .host("localhost".to_string())
            .port(8563)
            .username("user".to_string())
            .password("pass".to_string())
            .schema("schema".to_string())
            .build()
            .unwrap();

        let url = options.to_url_lossy();

        assert_eq!(url.scheme(), "exa");
        assert_eq!(url.host_str(), Some("localhost"));
        assert_eq!(url.port(), Some(8563));
        assert_eq!(url.path(), "/schema");
        assert_eq!(url.username(), "user");
        assert_eq!(url.password(), Some("pass"));
    }

    #[test]
    fn test_to_url_lossy_access_token() {
        let options = ExaConnectOptions::builder()
            .host("localhost".to_string())
            .access_token("token123".to_string())
            .build()
            .unwrap();

        let url = options.to_url_lossy();

        let query_pairs: std::collections::HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        assert_eq!(query_pairs.get(ACCESS_TOKEN), Some(&"token123".to_string()));
    }

    #[test]
    fn test_to_url_lossy_refresh_token() {
        let options = ExaConnectOptions::builder()
            .host("localhost".to_string())
            .refresh_token("refresh123".to_string())
            .build()
            .unwrap();

        let url = options.to_url_lossy();

        let query_pairs: std::collections::HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        assert_eq!(
            query_pairs.get(REFRESH_TOKEN),
            Some(&"refresh123".to_string())
        );
    }

    #[test]
    fn test_to_url_lossy_all_params() {
        let options = ExaConnectOptions::builder()
            .host("localhost".to_string())
            .port(8563)
            .username("user".to_string())
            .password("pass".to_string())
            .schema("schema".to_string())
            .compression_mode(ExaCompressionMode::Disabled)
            .fetch_size(2048)
            .query_timeout(60)
            .feedback_interval(5)
            .statement_cache_capacity(NonZeroUsize::new(200).unwrap())
            .build()
            .unwrap();

        let url = options.to_url_lossy();

        let query_pairs: std::collections::HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        assert_eq!(query_pairs.get(COMPRESSION), Some(&"disabled".to_string()));
        assert_eq!(query_pairs.get(FETCH_SIZE), Some(&"2048".to_string()));
        assert_eq!(query_pairs.get(QUERY_TIMEOUT), Some(&"60".to_string()));
        assert_eq!(query_pairs.get(FEEDBACK_INTERVAL), Some(&"5".to_string()));
        assert_eq!(
            query_pairs.get(STATEMENT_CACHE_CAPACITY),
            Some(&"200".to_string())
        );
    }

    #[test]
    fn test_roundtrip_conversion() {
        let original_url =
            "exa://user:pass@localhost:8563/schema?compression=preferred&fetch-size=1024";
        let options = ExaConnectOptions::from_str(original_url).unwrap();
        let reconstructed_url = options.to_url_lossy();
        let options2 = ExaConnectOptions::from_url(&reconstructed_url).unwrap();

        assert_eq!(options.hostname, options2.hostname);
        assert_eq!(options.port, options2.port);
        assert_eq!(options.schema, options2.schema);
        assert_eq!(options.compression_mode, options2.compression_mode);
        assert_eq!(options.fetch_size, options2.fetch_size);
    }
    #[test]
    fn test_compression_modes() {
        // Test ExaCompressionMode::Disabled
        let url = "exa://user:pass@localhost:8563?compression=disabled";
        let options = ExaConnectOptions::from_str(url).unwrap();
        assert_eq!(options.compression_mode, ExaCompressionMode::Disabled);

        // Test ExaCompressionMode::Preferred
        let url = "exa://user:pass@localhost:8563?compression=preferred";
        let options = ExaConnectOptions::from_str(url).unwrap();
        assert_eq!(options.compression_mode, ExaCompressionMode::Preferred);

        // Test ExaCompressionMode::Required
        let url = "exa://user:pass@localhost:8563?compression=required";
        let options = ExaConnectOptions::from_str(url).unwrap();
        assert_eq!(options.compression_mode, ExaCompressionMode::Required);
    }

    #[test]
    fn test_ssl_modes() {
        // Test ExaSslMode::Disable
        let url = "exa://user:pass@localhost:8563?ssl-mode=disabled";
        let options = ExaConnectOptions::from_str(url).unwrap();
        assert_eq!(options.ssl_mode, ExaSslMode::Disabled);

        // Test ExaSslMode::Preferred
        let url = "exa://user:pass@localhost:8563?ssl-mode=preferred";
        let options = ExaConnectOptions::from_str(url).unwrap();
        assert_eq!(options.ssl_mode, ExaSslMode::Preferred);

        // Test ExaSslMode::Required
        let url = "exa://user:pass@localhost:8563?ssl-mode=required";
        let options = ExaConnectOptions::from_str(url).unwrap();
        assert_eq!(options.ssl_mode, ExaSslMode::Required);
    }

    #[test]
    fn test_compression_and_ssl_modes_together() {
        let url = "exa://user:pass@localhost:8563?compression=required&ssl-mode=required";
        let options = ExaConnectOptions::from_str(url).unwrap();
        assert_eq!(options.compression_mode, ExaCompressionMode::Required);
        assert_eq!(options.ssl_mode, ExaSslMode::Required);
    }

    #[test]
    fn test_compression_mode_to_url_lossy() {
        // Test that compression modes are correctly serialized back to URL
        let options = ExaConnectOptions::builder()
            .host("localhost".to_string())
            .username("user".to_string())
            .password("pass".to_string())
            .compression_mode(ExaCompressionMode::Required)
            .build()
            .unwrap();

        let url = options.to_url_lossy();
        let query_pairs: std::collections::HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        assert_eq!(query_pairs.get(COMPRESSION), Some(&"required".to_string()));
    }

    #[test]
    fn test_ssl_mode_to_url_lossy() {
        // Test that SSL modes are correctly serialized back to URL
        let options = ExaConnectOptions::builder()
            .host("localhost".to_string())
            .username("user".to_string())
            .password("pass".to_string())
            .ssl_mode(ExaSslMode::Required)
            .build()
            .unwrap();

        let url = options.to_url_lossy();
        let query_pairs: std::collections::HashMap<String, String> =
            url.query_pairs().into_owned().collect();

        assert_eq!(query_pairs.get(SSL_MODE), Some(&"required".to_string()));
    }
}
