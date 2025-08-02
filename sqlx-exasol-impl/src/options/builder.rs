use std::{net::ToSocketAddrs, num::NonZeroUsize};

use sqlx_core::{connection::LogSettings, net::tls::CertificateInput};

use super::{
    error::ExaConfigError, ssl_mode::ExaSslMode, ExaConnectOptions, Login, ProtocolVersion,
    DEFAULT_CACHE_CAPACITY, DEFAULT_FETCH_SIZE, DEFAULT_PORT,
};
use crate::{options::compression::CompressionMode, SqlxResult};

/// Builder for [`ExaConnectOptions`].
#[derive(Clone, Debug)]
pub struct ExaConnectOptionsBuilder {
    host: Option<String>,
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
    compression_mode: CompressionMode,
    feedback_interval: u64,
}

impl Default for ExaConnectOptionsBuilder {
    fn default() -> Self {
        Self {
            host: None,
            port: DEFAULT_PORT,
            ssl_mode: ExaSslMode::default(),
            ssl_ca: None,
            ssl_client_cert: None,
            ssl_client_key: None,
            statement_cache_capacity: DEFAULT_CACHE_CAPACITY,
            username: None,
            password: None,
            access_token: None,
            refresh_token: None,
            schema: None,
            protocol_version: ProtocolVersion::V3,
            fetch_size: DEFAULT_FETCH_SIZE,
            query_timeout: 0,
            compression_mode: CompressionMode::default(),
            feedback_interval: 1,
        }
    }
}

impl ExaConnectOptionsBuilder {
    /// Consumes this builder and returns an instance of [`ExaConnectOptions`].
    ///
    /// # Errors
    ///
    /// Will return an error if resolving the hostname to [`std::net::SocketAddr`] fails.
    pub fn build(self) -> SqlxResult<ExaConnectOptions> {
        let hostname = self.host.ok_or(ExaConfigError::MissingHost)?;
        let password = self.password.unwrap_or_default();

        // Only one authentication method can be used at once
        let login = match (self.username, self.access_token, self.refresh_token) {
            (Some(username), None, None) => Login::Credentials { username, password },
            (None, Some(access_token), None) => Login::AccessToken { access_token },
            (None, None, Some(refresh_token)) => Login::RefreshToken { refresh_token },
            (None, None, None) => return Err(ExaConfigError::MissingAuthMethod.into()),
            _ => return Err(ExaConfigError::MultipleAuthMethods.into()),
        };

        let hosts = Self::parse_hostname(&hostname);
        let mut hosts_details = Vec::with_capacity(hosts.len());

        for host in hosts {
            let addrs = (host.as_str(), self.port).to_socket_addrs()?.collect();
            hosts_details.push((host, addrs));
        }

        let opts = ExaConnectOptions {
            hosts_details,
            port: self.port,
            ssl_mode: self.ssl_mode,
            ssl_ca: self.ssl_ca,
            ssl_client_cert: self.ssl_client_cert,
            ssl_client_key: self.ssl_client_key,
            statement_cache_capacity: self.statement_cache_capacity,
            hostname,
            login,
            schema: self.schema,
            protocol_version: self.protocol_version,
            fetch_size: self.fetch_size,
            query_timeout: self.query_timeout,
            compression_mode: self.compression_mode,
            feedback_interval: self.feedback_interval,
            log_settings: LogSettings::default(),
        };

        Ok(opts)
    }

    #[must_use = "call build() to get connection options"]
    pub fn host(mut self, host: String) -> Self {
        self.host = Some(host);
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn ssl_mode(mut self, ssl_mode: ExaSslMode) -> Self {
        self.ssl_mode = ssl_mode;
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn ssl_ca(mut self, ssl_ca: CertificateInput) -> Self {
        self.ssl_ca = Some(ssl_ca);
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn ssl_client_cert(mut self, ssl_client_cert: CertificateInput) -> Self {
        self.ssl_client_cert = Some(ssl_client_cert);
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn ssl_client_key(mut self, ssl_client_key: CertificateInput) -> Self {
        self.ssl_client_key = Some(ssl_client_key);
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn statement_cache_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.statement_cache_capacity = capacity;
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn username(mut self, username: String) -> Self {
        self.username = Some(username);
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn password(mut self, password: String) -> Self {
        self.password = Some(password);
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn access_token(mut self, access_token: String) -> Self {
        self.access_token = Some(access_token);
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn refresh_token(mut self, refresh_token: String) -> Self {
        self.refresh_token = Some(refresh_token);
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn schema(mut self, schema: String) -> Self {
        self.schema = Some(schema);
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn protocol_version(mut self, protocol_version: ProtocolVersion) -> Self {
        self.protocol_version = protocol_version;
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn fetch_size(mut self, fetch_size: usize) -> Self {
        self.fetch_size = fetch_size;
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn query_timeout(mut self, query_timeout: u64) -> Self {
        self.query_timeout = query_timeout;
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn compression_mode(mut self, compression_mode: CompressionMode) -> Self {
        self.compression_mode = compression_mode;
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn feedback_interval(mut self, feedback_interval: u64) -> Self {
        self.feedback_interval = feedback_interval;
        self
    }

    /// Exasol supports host ranges, e.g: hostname4..1.com.
    /// This method parses the provided host in the connection string and generates one for each
    /// possible entry in the range.
    ///
    /// We do expect the range to be in the ascending order though, so `hostname4..1.com` won't
    /// work.
    fn parse_hostname(hostname: &str) -> Vec<String> {
        // If multiple hosts could not be generated, then the given hostname is the only one.
        Self::_parse_hostname(hostname).unwrap_or_else(|| vec![hostname.to_owned()])
    }

    /// This method is used to attempt to generate multiple hosts out of the given hostname.
    ///
    /// If that fails, we'll bail early and unwrap the option in a wrapper.
    #[inline]
    fn _parse_hostname(hostname: &str) -> Option<Vec<String>> {
        let mut index_accum = 0;

        // We loop through occurences of ranges (..) and try to find one surrounded by digits.
        // If that happens, then we break out of the loop with the index of the range occurance.
        let range_idx = loop {
            let search_str = &hostname[index_accum..];

            // No range? No problem! Return early.
            let idx = search_str.find("..")?;

            // While someone actually using something like "..thisismyhostname" in the connection
            // string would be absolutely insane, it's still somewhat nicer not have this overflow.
            //
            // But really, if you read this and your host looks like that, you really should
            // re-evaluate your taste in domain names.
            //
            // In any case, the index points to the range dots.
            // We want to look before that, hence the substraction.
            let before_opt = idx
                .checked_sub(1)
                .and_then(|i| search_str.as_bytes().get(i));

            // Get the byte after the range dots.
            let after_opt = search_str.as_bytes().get(idx + 2);

            // Check if the range is surrounded by digits and if so, return its index.
            // Continue to the next range if not.
            break match (before_opt, after_opt) {
                (Some(b), Some(a)) if b.is_ascii_digit() || a.is_ascii_digit() => idx + index_accum,
                _ => {
                    index_accum += idx + 2;
                    continue;
                }
            };
        };

        let before_range = &hostname[..range_idx];
        let after_range = &hostname[range_idx + 2..];

        // We wanna find the last non-digit character before the range index in the first part of
        // the hostname and the first non-digit character right after the range dots, in the
        // second part of the hostname.
        //
        // The start is incremented as the index is for the last non-numeric character.
        //
        // If no indexes are found, then we consider the beginning/end of string, respectively.
        let start_idx = before_range
            .rfind(|c: char| !c.is_ascii_digit())
            .map(|i| i + 1)
            .unwrap_or_default();
        let end_idx = after_range
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after_range.len());

        // We split the hostname parts to isolate components.
        let (prefix, start_range) = before_range.split_at(start_idx);
        let (end_range, suffix) = after_range.split_at(end_idx);

        // Return the hostname as is if the range boundaries are not integers.
        let start = start_range.parse::<usize>().ok()?;
        let end = end_range.parse::<usize>().ok()?;

        let hosts = (start..=end)
            .map(|i| format!("{prefix}{i}{suffix}"))
            .collect();

        Some(hosts)
    }
}

#[cfg(test)]
mod tests {
    use super::ExaConnectOptionsBuilder;

    #[test]
    fn test_simple_ip() {
        let hostname = "10.10.10.10";

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, vec!(hostname));
    }

    #[test]
    fn test_ip_range_end() {
        let hostname = "10.10.10.1..3";
        let expected = vec!["10.10.10.1", "10.10.10.2", "10.10.10.3"];

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, expected);
    }

    #[test]
    fn test_ip_range_start() {
        let hostname = "1..3.10.10.10";
        let expected = vec!["1.10.10.10", "2.10.10.10", "3.10.10.10"];

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, expected);
    }

    #[test]
    fn test_simple_hostname() {
        let hostname = "myhost.com";

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, vec!(hostname));
    }

    #[test]
    fn test_hostname_with_range() {
        let hostname = "myhost1..4.com";
        let expected = vec!["myhost1.com", "myhost2.com", "myhost3.com", "myhost4.com"];

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, expected);
    }

    #[test]
    fn test_hostname_with_big_range() {
        let hostname = "myhost125..127.com";
        let expected = vec!["myhost125.com", "myhost126.com", "myhost127.com"];

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, expected);
    }

    #[test]
    fn test_hostname_with_inverse_range() {
        let hostname = "myhost127..125.com";

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert!(generated.is_empty());
    }

    #[test]
    fn test_hostname_with_numbers_no_range() {
        let hostname = "myhost1.4.com";

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, vec![hostname]);
    }

    #[test]
    fn test_hostname_with_range_one_numbers() {
        let hostname = "myhost1..b.com";

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, vec![hostname]);
    }

    #[test]
    fn test_hostname_with_range_no_numbers() {
        let hostname = "myhosta..b.com";

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, vec![hostname]);
    }

    #[test]
    fn test_hostname_starts_with_range() {
        let hostname = "..myhost.com";

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, vec![hostname]);
    }

    #[test]
    fn test_hostname_ends_with_range() {
        let hostname = "myhost.com..";

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, vec![hostname]);
    }

    #[test]
    fn test_hostname_real_and_fake_range() {
        let hostname = "myhosta..bcdef1..3.com";
        let expected = vec![
            "myhosta..bcdef1.com",
            "myhosta..bcdef2.com",
            "myhosta..bcdef3.com",
        ];

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, expected);
    }

    #[test]
    fn test_hostname_two_valid_ranges() {
        let hostname = "myhost1..3cdef4..7.com";
        let expected = vec![
            "myhost1cdef4..7.com",
            "myhost2cdef4..7.com",
            "myhost3cdef4..7.com",
        ];

        let generated = ExaConnectOptionsBuilder::parse_hostname(hostname);
        assert_eq!(generated, expected);
    }
}
