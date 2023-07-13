use std::{
    env,
    net::{SocketAddr, ToSocketAddrs},
};

use crate::error::{ConResult, ConnectionError};

use super::{ExaConnectOptions, LoginKind, ProtocolVersion};
use lazy_regex::regex;
use rand::seq::SliceRandom;

#[derive(Clone, Debug)]
pub struct ConOptsBuilder<const DSN: bool, const LOGIN: bool> {
    addresses: Vec<SocketAddr>,
    fingerprint: Option<String>,
    login_kind: Option<LoginKind>,
    schema: Option<String>,
    protocol_version: ProtocolVersion,
    client_name: String,
    client_version: String,
    client_os: String,
    fetch_size: usize,
    query_timeout: u64,
    use_encryption: bool,
    use_compression: bool,
    lowercase_columns: bool,
    autocommit: bool,
}

impl ConOptsBuilder<false, false> {
    pub(crate) fn new() -> Self {
        let crate_version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "UNKNOWN".to_owned());
        let use_encryption = cfg!(any(feature = "native-tls-basic", feature = "rustls"));
        let use_compression = cfg!(feature = "flate2");

        Self {
            addresses: Vec::new(),
            fingerprint: None,
            login_kind: None,
            schema: None,
            protocol_version: ProtocolVersion::V3,
            client_name: format!("{} {}", "Rust Exasol", crate_version),
            client_version: crate_version,
            client_os: env::consts::OS.to_owned(),
            fetch_size: Self::DEFAULT_FETCH_SIZE,
            query_timeout: 0,
            use_encryption,
            use_compression,
            lowercase_columns: true,
            autocommit: true,
        }
    }
}

impl ConOptsBuilder<true, true> {
    pub fn build(self) -> ExaConnectOptions {
        ExaConnectOptions {
            addresses: self.addresses,
            fingerprint: self.fingerprint,
            login_kind: self.login_kind.unwrap(),
            schema: self.schema,
            protocol_version: self.protocol_version,
            client_name: self.client_name,
            client_version: self.client_version,
            client_os: self.client_os,
            fetch_size: self.fetch_size,
            query_timeout: self.query_timeout,
            use_encryption: self.use_encryption,
            use_compression: self.use_compression,
            lowercase_columns: self.lowercase_columns,
            autocommit: self.autocommit,
        }
    }
}

impl<const DSN: bool, const LOGIN: bool> ConOptsBuilder<DSN, LOGIN> {
    const DEFAULT_PORT: u16 = 8563;
    const DEFAULT_FETCH_SIZE: usize = 5 * 1024 * 1024;

    pub fn dsn(self, dsn: &str) -> ConResult<ConOptsBuilder<true, LOGIN>> {
        let (addresses, fingerprint) = Self::parse_dsn(dsn)?;

        let builder = ConOptsBuilder {
            addresses,
            fingerprint,
            login_kind: self.login_kind,
            schema: self.schema,
            protocol_version: self.protocol_version,
            client_name: self.client_name,
            client_version: self.client_version,
            client_os: self.client_os,
            fetch_size: self.fetch_size,
            query_timeout: self.query_timeout,
            use_encryption: self.use_encryption,
            use_compression: self.use_compression,
            lowercase_columns: self.lowercase_columns,
            autocommit: self.autocommit,
        };

        Ok(builder)
    }

    pub fn login_kind(self, login_kind: LoginKind) -> ConOptsBuilder<DSN, true> {
        ConOptsBuilder {
            addresses: self.addresses,
            fingerprint: self.fingerprint,
            login_kind: Some(login_kind),
            schema: self.schema,
            protocol_version: self.protocol_version,
            client_name: self.client_name,
            client_version: self.client_version,
            client_os: self.client_os,
            fetch_size: self.fetch_size,
            query_timeout: self.query_timeout,
            use_encryption: self.use_encryption,
            use_compression: self.use_compression,
            lowercase_columns: self.lowercase_columns,
            autocommit: self.autocommit,
        }
    }

    #[inline]
    pub fn schema(self, schema: String) -> Self {
        Self {
            schema: Some(schema),
            ..self
        }
    }

    #[inline]
    pub fn protocol_version(self, protocol_version: ProtocolVersion) -> Self {
        Self {
            protocol_version,
            ..self
        }
    }

    /// Data fetch size in bytes (defaults to `5,242,880 = 5 * 1024 * 1024`).
    #[inline]
    pub fn fetch_size(self, fetch_size: usize) -> Self {
        Self { fetch_size, ..self }
    }

    /// Query timeout (defaults to `0`, which means it's disabled).
    #[inline]
    pub fn query_timeout(self, query_timeout: u64) -> Self {
        Self {
            query_timeout,
            ..self
        }
    }

    /// Encryption flag (defaults to `true`).
    #[inline]
    #[cfg(any(feature = "native-tls-basic", feature = "rustls"))]
    pub fn use_encryption(self, use_encryption: bool) -> Self {
        Self {
            use_encryption,
            ..self
        }
    }

    /// Compression flag (defaults to `true`).
    #[inline]
    #[cfg(feature = "flate2")]
    pub fn use_compression(self, use_compression: bool) -> Self {
        Self {
            use_compression,
            ..self
        }
    }

    /// Lowercase column names flag (defaults to `true`)
    #[inline]
    pub fn lowercase_columns(self, lowercase_columns: bool) -> Self {
        Self {
            lowercase_columns,
            ..self
        }
    }

    /// Autocommit flag (defaults to `true`).
    #[inline]
    pub fn autocommit(self, autocommit: bool) -> Self {
        Self { autocommit, ..self }
    }

    /// Parses the provided DSN to expand ranges and resolve IP addresses.
    /// If a fingerprint is found, it also gets returned.
    fn parse_dsn(dsn: &str) -> ConResult<(Vec<SocketAddr>, Option<String>)> {
        let re = regex!(
            r"(?x)
                    ^(.+?)                     # Hostname prefix
                    (?:(\d+)\.\.(\d+)(.*?))?   # Optional range and hostname suffix (e.g. hostname1..4.com)
                    (?:/([0-9A-Fa-f]+))?       # Optional fingerprint (e.g. hostname1..4.com/135a1d2dce102de866f58267521f4232153545a075dc85f8f7596f57e588a181)
                    (?::(\d+)?)?$              # Optional port (e.g. hostname1..4.com:8564)
                    "
        );

        let caps = re.captures(dsn).ok_or(ConnectionError::InvalidDSN)?;

        // Parse capture groups from regex
        let hostname_prefix = caps.get(1).ok_or(ConnectionError::InvalidDSN)?.as_str();
        let start_range = caps.get(2).map(|m| m.as_str().parse::<u8>()).transpose()?;
        let end_range = caps.get(3).map(|m| m.as_str().parse()).transpose()?;
        let hostname_suffix = caps.get(4).map(|m| m.as_str()).unwrap_or_default();
        let fingerprint = caps.get(5).map(|m| m.as_str().to_owned());
        let port = caps
            .get(6)
            .map(|m| m.as_str().parse())
            .transpose()?
            .unwrap_or(Self::DEFAULT_PORT);

        // Create a new vec for storing hosts
        let mut hosts = Vec::new();

        match (start_range, end_range) {
            // Ranges were provided so generate all possible hostnames
            (Some(start_range), Some(end_range)) => (start_range..end_range).for_each(|i| {
                let host = format!("{}{}{}:{}", hostname_prefix, i, hostname_suffix, port);
                hosts.push(host);
            }),

            // No range provided so there's a single possible host
            (None, None) => {
                let host = format!("{}{}:{}", hostname_prefix, hostname_suffix, port);
                hosts.push(host);
            }
            _ => return Err(ConnectionError::InvalidDSN),
        }

        // We have to resolve hostnames to IPs
        let fold_fn = |mut accum: Vec<SocketAddr>, elem: String| {
            elem.to_socket_addrs()
                .map(|v| accum.extend(v))
                .map(|_| accum)
        };

        let mut addresses = hosts.into_iter().try_fold(Vec::new(), fold_fn)?;
        addresses.shuffle(&mut rand::thread_rng());

        Ok((addresses, fingerprint))
    }
}
