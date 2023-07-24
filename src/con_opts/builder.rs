use super::{
    login::{AccessToken, RefreshToken},
    Credentials, ExaConnectOptions, Login, ProtocolVersion,
};
use sqlx_core::connection::LogSettings;

#[derive(Clone, Debug)]
pub struct ExaConnectOptionsBuilder<'a> {
    host: Option<&'a str>,
    port: u16,
    username: Option<String>,
    password: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
    schema: Option<String>,
    protocol_version: ProtocolVersion,
    fetch_size: usize,
    query_timeout: u64,
    encryption: bool,
    compression: bool,
}

impl<'a> ExaConnectOptionsBuilder<'a> {
    const DEFAULT_FETCH_SIZE: usize = 5 * 1024 * 1024;
    const DEFAULT_PORT: u16 = 8563;

    pub(crate) fn new() -> Self {
        let use_encryption = cfg!(any(feature = "native-tls-basic", feature = "rustls"));
        let use_compression = cfg!(feature = "flate2");

        Self {
            host: None,
            port: Self::DEFAULT_PORT,
            username: None,
            password: None,
            access_token: None,
            refresh_token: None,
            schema: None,
            protocol_version: ProtocolVersion::V3,
            fetch_size: Self::DEFAULT_FETCH_SIZE,
            query_timeout: 0,
            encryption: use_encryption,
            compression: use_compression,
        }
    }
}

impl<'a> ExaConnectOptionsBuilder<'a> {
    const WS_SCHEME: &str = "ws";
    const WSS_SCHEME: &str = "wss";

    pub fn build(self) -> Result<ExaConnectOptions, String> {
        let scheme = match self.encryption {
            true => Self::WSS_SCHEME,
            false => Self::WS_SCHEME,
        };

        let Some(hostname) = self.host else {return Err("No hostname provided".to_owned())};

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
            hosts: Self::generate_hosts(scheme, hostname)?,
            port: self.port,
            login: login_kind,
            schema: self.schema,
            protocol_version: self.protocol_version,
            fetch_size: self.fetch_size,
            query_timeout: self.query_timeout,
            encryption: self.encryption,
            compression: self.compression,
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

    pub fn encryption(&mut self, encryption: bool) -> &mut Self {
        self.encryption = encryption;
        self
    }

    pub fn compression(&mut self, compression: bool) -> &mut Self {
        self.compression = compression;
        self
    }

    fn generate_hosts(scheme: &str, hostname: &str) -> Result<Vec<String>, String> {
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
            .map(|i| format!("{scheme}://{prefix}{i}{suffix}"))
            .collect();

        Ok(hosts)
    }
}
