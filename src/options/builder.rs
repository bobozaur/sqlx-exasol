use std::num::NonZeroUsize;

use super::{
    login::{AccessToken, RefreshToken},
    ssl_mode::ExaSslMode,
    Credentials, ExaConnectOptions, Login, ProtocolVersion, DEFAULT_CACHE_CAPACITY,
    DEFAULT_FETCH_SIZE, DEFAULT_PORT,
};
use sqlx_core::{connection::LogSettings, net::tls::CertificateInput};

#[derive(Clone, Debug)]
pub struct ExaConnectOptionsBuilder<'a> {
    host: Option<&'a str>,
    port: u16,
    ssl_mode: ExaSslMode,
    ssl_ca: Option<CertificateInput>,
    ssl_client_cert: Option<CertificateInput>,
    ssl_client_key: Option<CertificateInput>,
    statement_cache_capacity: NonZeroUsize,
    username: Option<String>,
    password: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
    schema: Option<String>,
    protocol_version: ProtocolVersion,
    fetch_size: usize,
    query_timeout: u64,
    compression: bool,
    feedback_interval: u8,
}

impl<'a> ExaConnectOptionsBuilder<'a> {
    pub(crate) fn new() -> Self {
        Self {
            host: None,
            port: DEFAULT_PORT,
            ssl_mode: ExaSslMode::default(),
            ssl_ca: None,
            ssl_client_cert: None,
            ssl_client_key: None,
            statement_cache_capacity: NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).unwrap(),
            username: None,
            password: None,
            access_token: None,
            refresh_token: None,
            schema: None,
            protocol_version: ProtocolVersion::V3,
            fetch_size: DEFAULT_FETCH_SIZE,
            query_timeout: 0,
            compression: false,
            feedback_interval: 1,
        }
    }
}

impl<'a> ExaConnectOptionsBuilder<'a> {
    pub fn build(self) -> Result<ExaConnectOptions, String> {
        let Some(hostname) = self.host else {
            return Err("No hostname provided".to_owned());
        };

        let login_kind = match (self.username, self.access_token, self.refresh_token) {
            (Some(username), None, None) => Login::Credentials(Credentials::new(
                username,
                self.password.unwrap_or_default(),
            )),
            (None, Some(access_token), None) => Login::AccessToken(AccessToken::new(access_token)),
            (None, None, Some(refresh_token)) => {
                Login::RefreshToken(RefreshToken::new(refresh_token))
            }
            _ => return Err("Multiple auth methods provided".to_owned()),
        };

        let opts = ExaConnectOptions {
            hosts: Self::generate_hosts(hostname)?,
            port: self.port,
            ssl_mode: self.ssl_mode,
            ssl_ca: self.ssl_ca,
            ssl_client_cert: self.ssl_client_cert,
            ssl_client_key: self.ssl_client_key,
            statement_cache_capacity: self.statement_cache_capacity,
            login: login_kind,
            schema: self.schema,
            protocol_version: self.protocol_version,
            fetch_size: self.fetch_size,
            query_timeout: self.query_timeout,
            compression: self.compression,
            feedback_interval: self.feedback_interval,
            log_settings: LogSettings::default(),
        };

        Ok(opts)
    }

    pub fn host(&mut self, host: &'a str) -> &mut Self {
        self.host = Some(host);
        self
    }

    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port = port;
        self
    }

    pub fn ssl_mode(&mut self, ssl_mode: ExaSslMode) -> &mut Self {
        self.ssl_mode = ssl_mode;
        self
    }

    pub fn ssl_ca(&mut self, ssl_ca: CertificateInput) -> &mut Self {
        self.ssl_ca = Some(ssl_ca);
        self
    }

    pub fn ssl_client_cert(&mut self, ssl_client_cert: CertificateInput) -> &mut Self {
        self.ssl_client_cert = Some(ssl_client_cert);
        self
    }

    pub fn ssl_client_key(&mut self, ssl_client_key: CertificateInput) -> &mut Self {
        self.ssl_client_key = Some(ssl_client_key);
        self
    }

    pub fn statement_cache_capacity(&mut self, capacity: NonZeroUsize) -> &mut Self {
        self.statement_cache_capacity = capacity;
        self
    }

    pub fn username(&mut self, username: String) -> &mut Self {
        self.username = Some(username);
        self
    }

    pub fn password(&mut self, password: String) -> &mut Self {
        self.password = Some(password);
        self
    }

    pub fn access_token(&mut self, access_token: String) -> &mut Self {
        self.access_token = Some(access_token);
        self
    }

    pub fn refresh_token(&mut self, refresh_token: String) -> &mut Self {
        self.refresh_token = Some(refresh_token);
        self
    }

    pub fn schema(&mut self, schema: String) -> &mut Self {
        self.schema = Some(schema);
        self
    }

    pub fn protocol_version(&mut self, protocol_version: ProtocolVersion) -> &mut Self {
        self.protocol_version = protocol_version;
        self
    }

    pub fn fetch_size(&mut self, fetch_size: usize) -> &mut Self {
        self.fetch_size = fetch_size;
        self
    }

    pub fn query_timeout(&mut self, query_timeout: u64) -> &mut Self {
        self.query_timeout = query_timeout;
        self
    }

    pub fn compression(&mut self, compression: bool) -> &mut Self {
        self.compression = compression;
        self
    }

    pub fn feedback_interval(&mut self, feedback_interval: u8) -> &mut Self {
        self.feedback_interval = feedback_interval;
        self
    }

    fn generate_hosts(hostname: &str) -> Result<Vec<String>, String> {
        let mut hostname_iter = hostname.split("..");

        let (first, last) = match (hostname_iter.next(), hostname_iter.next()) {
            (Some(first), Some(last)) => (first, last),
            _ => return Ok(vec![hostname.to_owned()]),
        };

        let (start_range_idx, end_range_idx) = match (
            first.find(char::is_numeric),
            last.find(|c: char| !c.is_numeric()),
        ) {
            (Some(start), Some(end)) => (start, end),
            _ => return Ok(vec![hostname.to_owned()]),
        };

        let (prefix, start_range) = first.split_at(start_range_idx);
        let (end_range, suffix) = last.split_at(end_range_idx);

        let start_range = start_range.parse::<usize>().map_err(|e| e.to_string())?;
        let end_range = end_range.parse::<usize>().map_err(|e| e.to_string())?;

        let hosts = (start_range..end_range)
            .map(|i| format!("{prefix}{i}{suffix}"))
            .collect();

        Ok(hosts)
    }
}
