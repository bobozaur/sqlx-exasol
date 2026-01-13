use std::ops::RangeInclusive;

use sqlx_core::{connection::LogSettings, net::tls::CertificateInput};

use super::{
    error::ExaConfigError, ssl_mode::ExaSslMode, ExaConnectOptions, Login, ProtocolVersion,
    DEFAULT_CACHE_CAPACITY, DEFAULT_FETCH_SIZE, DEFAULT_PORT,
};
use crate::{options::compression::ExaCompressionMode, SqlxResult};

/// Builder for [`ExaConnectOptions`].
#[derive(Clone, Debug)]
pub struct ExaConnectOptionsBuilder {
    url_host: Option<String>,
    url_port: u16,
    extra_hosts: Vec<(String, u16)>,
    ssl_mode: ExaSslMode,
    ssl_ca: Option<CertificateInput>,
    ssl_client_cert: Option<CertificateInput>,
    ssl_client_key: Option<CertificateInput>,
    statement_cache_capacity: usize,
    username: Option<String>,
    password: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
    schema: Option<String>,
    protocol_version: ProtocolVersion,
    fetch_size: usize,
    query_timeout: u64,
    compression_mode: ExaCompressionMode,
    feedback_interval: u64,
}

impl Default for ExaConnectOptionsBuilder {
    #[must_use = "call build() to get connection options"]
    fn default() -> Self {
        Self {
            url_host: None,
            url_port: DEFAULT_PORT,
            extra_hosts: Vec::new(),
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
            protocol_version: ProtocolVersion::default(),
            fetch_size: DEFAULT_FETCH_SIZE,
            query_timeout: 0,
            compression_mode: ExaCompressionMode::default(),
            feedback_interval: 1,
        }
    }
}

impl ExaConnectOptionsBuilder {
    /// Consumes this builder and returns an instance of [`ExaConnectOptions`].
    ///
    /// # Errors
    ///
    /// Will return an error if no host or other than exactly one login method were provided.
    pub fn build(self) -> SqlxResult<ExaConnectOptions> {
        let url_host = self.url_host.ok_or(ExaConfigError::MissingHost)?;
        let password = self.password.unwrap_or_default();

        // Only one authentication method can be used at once
        let login = match (self.username, self.access_token, self.refresh_token) {
            (Some(username), None, None) => Login::Credentials { username, password },
            (None, Some(access_token), None) => Login::AccessToken { access_token },
            (None, None, Some(refresh_token)) => Login::RefreshToken { refresh_token },
            (None, None, None) => return Err(ExaConfigError::MissingAuthMethod.into()),
            _ => return Err(ExaConfigError::MultipleAuthMethods.into()),
        };

        let hosts = Some((url_host.clone(), self.url_port))
            .into_iter()
            .chain(self.extra_hosts)
            .flat_map(|(host, port)| Self::parse_host(host).map(move |host| (host.into(), port)))
            .collect();

        let opts = ExaConnectOptions {
            hosts,
            ssl_mode: self.ssl_mode,
            ssl_ca: self.ssl_ca,
            ssl_client_cert: self.ssl_client_cert,
            ssl_client_key: self.ssl_client_key,
            statement_cache_capacity: self.statement_cache_capacity,
            url_host,
            url_port: self.url_port,
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
        self.url_host = Some(host);
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn port(mut self, port: u16) -> Self {
        self.url_port = port;
        self
    }

    /// Appends an additional host to be used for randomly
    /// connecting to Exasol nodes.
    ///
    /// Can be called multiple times.
    #[must_use = "call build() to get connection options"]
    pub fn extra_host(mut self, host: String, port: Option<u16>) -> Self {
        self.extra_hosts.push((host, port.unwrap_or(DEFAULT_PORT)));
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

    /// Sets the capacity of the statement cache.
    ///
    /// The cache is enabled by default. Setting the capacity to `0` disables the cache.
    #[must_use = "call build() to get connection options"]
    pub fn statement_cache_capacity(mut self, capacity: usize) -> Self {
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
    pub fn compression_mode(mut self, compression_mode: ExaCompressionMode) -> Self {
        self.compression_mode = compression_mode;
        self
    }

    #[must_use = "call build() to get connection options"]
    pub fn feedback_interval(mut self, feedback_interval: u64) -> Self {
        self.feedback_interval = feedback_interval;
        self
    }

    /// Exasol supports host ranges, e.g: hostname1..4.com.
    /// This method parses the provided host in the connection string and generates one for each
    /// possible entry in the range.
    ///
    /// We do expect the range to be in the ascending order though, so `hostname4..1.com` will be
    /// returned as is.
    fn parse_host(host: String) -> HostKind {
        // Loop through occurences of ranges (..) in reverse looking for one surrounded by digits.
        for (idx, _) in host.rmatch_indices("..") {
            let has_digit_before_range = idx
                .checked_sub(1)
                .and_then(|i| host.as_bytes().get(i))
                .is_some_and(u8::is_ascii_digit);

            let has_digit_after_range =
                host.as_bytes().get(idx + 2).is_some_and(u8::is_ascii_digit);

            // Move on if the range is not surrounded by digits.
            if !has_digit_before_range || !has_digit_after_range {
                continue;
            }

            let before_range = &host[..idx];
            let after_range = &host[idx + 2..];

            // Find the last non-digit character before the range index in the first part of
            // the hostname and the first non-digit character right after the range dots, in the
            // second part of the hostname.
            //
            // The start is incremented as the index is for the last non-numeric character.
            //
            // If no indexes are found, then we consider the beginning/end of string, respectively.
            let prefix_idx = before_range
                .rfind(|c: char| !c.is_ascii_digit())
                .map(|i| i + 1)
                .unwrap_or_default();
            let suffix_idx = after_range
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(after_range.len());

            // Split the hostname parts to isolate components.
            let (_, start_range) = before_range.split_at(prefix_idx);
            let (end_range, _) = after_range.split_at(suffix_idx);

            return match (start_range.parse::<usize>(), end_range.parse::<usize>()) {
                (Ok(start), Ok(end)) if start < end => HostKind::Range {
                    buffer: host,
                    prefix_idx,
                    suffix_idx: idx + 2 + suffix_idx,
                    range: start..=end,
                },
                // Return the hostname as is if the range boundaries are not integers
                // or if start >= end.
                _ => HostKind::Single(host),
            };
        }

        // No numeric range present, return singular hostname.
        HostKind::Single(host)
    }
}

#[derive(Clone, Debug, PartialEq)]
enum HostKind {
    Single(String),
    Range {
        buffer: String,
        prefix_idx: usize,
        suffix_idx: usize,
        range: RangeInclusive<usize>,
    },
}

impl Iterator for HostKind {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            HostKind::Single(s) => (!s.is_empty()).then(|| std::mem::take(s)),
            HostKind::Range {
                buffer,
                prefix_idx,
                suffix_idx,
                range,
            } => range.next().map(|i| {
                let (prefix, _) = buffer.split_at(*prefix_idx);
                let (_, suffix) = buffer.split_at(*suffix_idx);
                format!("{prefix}{i}{suffix}")
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_ip() {
        let host = "10.10.10.10";
        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(Some(host.to_owned())));
    }

    #[test]
    fn test_ip_range_end() {
        let host = "10.10.10.1..3";
        let expected = vec![
            "10.10.10.1".to_owned(),
            "10.10.10.2".to_owned(),
            "10.10.10.3".to_owned(),
        ];

        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(expected));
    }

    #[test]
    fn test_ip_range_start() {
        let host = "1..3.10.10.10";
        let expected = vec![
            "1.10.10.10".to_owned(),
            "2.10.10.10".to_owned(),
            "3.10.10.10".to_owned(),
        ];

        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(expected));
    }

    #[test]
    fn test_simple_hostname() {
        let host = "myhost.com";
        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(Some(host.to_owned())));
    }

    #[test]
    fn test_hostname_with_range() {
        let host = "myhost1..4.com";
        let expected = vec![
            "myhost1.com".to_owned(),
            "myhost2.com".to_owned(),
            "myhost3.com".to_owned(),
            "myhost4.com".to_owned(),
        ];

        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(expected));
    }

    #[test]
    fn test_hostname_with_big_range() {
        let host = "myhost125..127.com";
        let expected = vec![
            "myhost125.com".to_owned(),
            "myhost126.com".to_owned(),
            "myhost127.com".to_owned(),
        ];

        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(expected));
    }

    #[test]
    fn test_hostname_with_inverse_range() {
        let host = "myhost127..125.com";
        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(Some(host.to_owned())));
    }

    #[test]
    fn test_hostname_with_numbers_no_range() {
        let host = "myhost1.4.com";
        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(Some(host.to_owned())));
    }

    #[test]
    fn test_hostname_with_range_one_numbers() {
        let host = "myhost1..b.com";
        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(Some(host.to_owned())));
    }

    #[test]
    fn test_hostname_with_range_no_numbers() {
        let host = "myhosta..b.com";
        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(Some(host.to_owned())));
    }

    #[test]
    fn test_hostname_starts_with_range() {
        let host = "..myhost.com";
        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(Some(host.to_owned())));
    }

    #[test]
    fn test_hostname_ends_with_range() {
        let host = "myhost.com..";
        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(Some(host.to_owned())));
    }

    #[test]
    fn test_hostname_real_and_fake_range() {
        let host = "myhosta..bcdef1..3.com";
        let expected = vec![
            "myhosta..bcdef1.com".to_owned(),
            "myhosta..bcdef2.com".to_owned(),
            "myhosta..bcdef3.com".to_owned(),
        ];

        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(expected));
    }

    #[test]
    fn test_hostname_two_valid_ranges() {
        let host = "myhost1..3cdef4..7.com";
        let expected = vec![
            "myhost1..3cdef4.com".to_owned(),
            "myhost1..3cdef5.com".to_owned(),
            "myhost1..3cdef6.com".to_owned(),
            "myhost1..3cdef7.com".to_owned(),
        ];

        let generated = ExaConnectOptionsBuilder::parse_host(host.to_owned());
        assert!(generated.eq(expected));
    }
}
