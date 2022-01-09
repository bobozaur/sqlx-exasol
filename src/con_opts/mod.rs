mod address;
mod credentials;
mod protocol_version;

use crate::error::ConnectionError;
use address::AddressList;
pub use credentials::{Credentials, LoginKind};
use lazy_regex::regex;
pub use protocol_version::ProtocolVersion;
use regex::Captures;
use rsa::RsaPublicKey;
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde_json::json;
use std::collections::HashMap;
use std::env;

const FIXED_CON_OPTS_LEN: usize = 7;

// Convenience alias
type ConResult<T> = std::result::Result<T, ConnectionError>;

/// Connection options for [Connection](crate::Connection)
/// The DSN may or may not contain a port - if it does not,
/// the port field in this struct is used as a fallback.
///
/// Default is implemented for [ConOpts] so that most fields have fallback values.
/// DSN, user, and password fields are practically mandatory,
/// as they otherwise default to an empty string or JSON null.
///
/// # Defaults
///
/// schema: None
/// port: 8563
/// protocol_version: ProtocolVersion::V3
/// fetch_size: 5 * 1024 * 1024
/// query_timeout: 0
/// use_encryption: *if encryption features are enabled true, else false*
/// use_compression: false
/// lowercase_columns: true
/// autocommit: true
///
/// ```
///  use exasol::{ConOpts, LoginKind, Credentials};
///
///  let mut opts = ConOpts::new(); // calls default() under the hood
///  opts.set_dsn("test_dsn");
///  opts.set_login_kind(LoginKind::Credentials(Credentials::new("test_user", "test_password")));
///  opts.set_schema(Some("test_schema"));
///  opts.set_autocommit(false);
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConOpts {
    dsn: Option<String>,
    login_kind: LoginKind,
    schema: Option<String>,
    port: u16,
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

impl Default for ConOpts {
    fn default() -> Self {
        let crate_version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "UNKNOWN".to_owned());
        let encryption = cfg!(any(feature = "native-tls-basic", feature = "rustls"));

        Self {
            dsn: None,
            login_kind: LoginKind::default(),
            schema: None,
            port: 8563,
            protocol_version: ProtocolVersion::V3,
            client_name: format!("{} {}", "Rust Exasol", crate_version),
            client_version: crate_version,
            client_os: env::consts::OS.to_owned(),
            fetch_size: 5 * 1024 * 1024,
            query_timeout: 0,
            use_encryption: encryption,
            use_compression: false,
            lowercase_columns: true,
            autocommit: true,
        }
    }
}

/// Connection options
impl ConOpts {
    /// Creates a default implementation of [ConOpts]
    pub fn new() -> Self {
        Self::default()
    }

    /// DSN (defaults to `None`)
    #[inline]
    pub fn dsn(&self) -> Option<&str> {
        self.dsn.as_deref()
    }

    #[inline]
    pub fn set_dsn<T>(&mut self, dsn: T)
    where
        T: Into<String>,
    {
        self.dsn = Some(dsn.into())
    }

    /// Port (defaults to `8563`).
    #[inline]
    pub fn port(&self) -> u16 {
        self.port
    }

    #[inline]
    pub fn set_port(&mut self, port: u16) {
        self.port = port
    }

    /// Schema (defaults to `None`).
    #[inline]
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    #[inline]
    pub fn set_schema<T>(&mut self, schema: Option<T>)
    where
        T: Into<String>,
    {
        self.schema = schema.map(|s| s.into())
    }

    /// User (defaults to `None`).
    #[inline]
    pub fn login_kind(&self) -> &LoginKind {
        &self.login_kind
    }

    #[inline]
    pub fn set_login_kind(&mut self, creds: LoginKind) {
        self.login_kind = creds
    }

    /// Protocol Version (defaults to `ProtocolVersion::V3`).
    #[inline]
    pub fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    #[inline]
    pub fn set_protocol_version(&mut self, pv: ProtocolVersion) {
        self.protocol_version = pv
    }

    /// Data fetch size in bytes (defaults to `5,242,880 = 5 * 1024 * 1024`).
    #[inline]
    pub fn fetch_size(&self) -> usize {
        self.fetch_size
    }

    #[inline]
    pub fn set_fetch_size(&mut self, fetch_size: usize) {
        self.fetch_size = fetch_size
    }

    /// Query timeout (defaults to `0`, which means it's disabled).
    #[inline]
    pub fn query_timeout(&self) -> u64 {
        self.query_timeout
    }

    #[inline]
    pub fn set_query_timeout(&mut self, timeout: u64) {
        self.query_timeout = timeout
    }

    /// Encryption flag (defaults to `false`).
    #[inline]
    pub fn encryption(&self) -> bool {
        self.use_encryption
    }

    /// Sets encryption by allowing the use of a secure websocket (WSS).
    #[inline]
    #[allow(unused)]
    pub fn set_encryption(&mut self, flag: bool) {
        if cfg!(any(feature = "native-tls-basic", feature = "rustls")) {
            self.use_encryption = flag;
        } else {
            panic!("native-tls or rustls features must be enabled to set encryption")
        }
    }

    /// Compression flag (defaults to `false`).
    #[inline]
    pub fn compression(&self) -> bool {
        self.use_compression
    }

    /// Sets the compression flag.
    #[inline]
    #[allow(unused)]
    pub fn set_compression(&mut self, flag: bool) {
        if cfg!(feature = "flate2") {
            self.use_compression = flag;
        } else {
            panic!("flate2 feature must be enabled to set compression")
        }
    }

    /// Lowercase column names flag (defaults to `true`)
    #[inline]
    pub fn lowercase_columns(&self) -> bool {
        self.lowercase_columns
    }

    #[inline]
    pub fn set_lowercase_columns(&mut self, flag: bool) {
        self.lowercase_columns = flag;
    }

    /// Autocommit flag (defaults to `true`).
    #[inline]
    pub fn autocommit(&self) -> bool {
        self.autocommit
    }

    /// Sets the autocommit flag
    #[inline]
    pub fn set_autocommit(&mut self, flag: bool) {
        self.autocommit = flag
    }

    /// Encrypts the underlying password if there is one.
    /// Access or refresh tokens could be used instead.
    #[inline]
    pub(crate) fn encrypt_password(&mut self, key: RsaPublicKey) -> ConResult<()> {
        self.login_kind.encrypt_password(key)
    }

    /// Convenience method for determining the websocket type.
    #[inline]
    pub(crate) fn ws_prefix(&self) -> &str {
        match self.encryption() {
            false => "ws",
            true => "wss",
        }
    }

    /// Parses the provided DSN to expand ranges and resolve IP addresses.
    /// Connection to all nodes will then be attempted in a random order
    /// until one is successful or all failed.
    pub(crate) fn parse_dsn(&self) -> ConResult<AddressList> {
        let re = regex!(
            r"(?x)
                    ^(.+?)                     # Hostname prefix
                    (?:(\d+)\.\.(\d+)(.*?))?   # Optional range and hostname suffix (e.g. hostname1..4.com)
                    (?:/([0-9A-Fa-f]+))?       # Optional fingerprint (e.g. hostname1..4.com/135a1d2dce102de866f58267521f4232153545a075dc85f8f7596f57e588a181)
                    (?::(\d+)?)?$              # Optional port (e.g. hostname1..4.com:8564)
                    "
        );

        self.dsn
            .as_deref()
            .and_then(|dsn| re.captures(dsn))
            .ok_or(ConnectionError::InvalidDSN)
            .and_then(|cap| {
                // Parse capture groups from regex
                let hostname_prefix = &cap[1];
                let start_range = Self::parse_dsn_part(&cap, 2);
                let end_range = Self::parse_dsn_part(&cap, 3);
                let hostname_suffix = Self::parse_dsn_part(&cap, 4);
                let fingerprint = Self::parse_dsn_part(&cap, 5).to_owned();
                let port = Self::parse_dsn_part(&cap, 6)
                    .parse::<u16>()
                    .unwrap_or(self.port);

                // Create a new vec for storing hosts
                let mut hosts = Vec::new();

                // If there's no start range, then there's a single possible host
                if start_range.is_empty() {
                    hosts.push(format!("{}{}:{}", hostname_prefix, hostname_suffix, port));
                } else {
                    // If ranges were provided, then generate all possible hostnames
                    let start_range = start_range.parse::<u8>()?;
                    let end_range = end_range.parse::<u8>()?;

                    // The order does not even matter
                    // Rust's range will increment/decrement appropriately :]
                    for i in start_range..end_range {
                        hosts.push(format!(
                            "{}{}{}:{}",
                            hostname_prefix, i, hostname_suffix, port
                        ))
                    }
                }

                AddressList::new(hosts, port, fingerprint)
            })
    }

    /// Used for retrieving an optional DSN part, or an empty string if missing
    #[inline]
    fn parse_dsn_part<'a>(cap: &'a Captures, index: usize) -> &'a str {
        cap.get(index).map_or("", |s| s.as_str())
    }
}

impl Serialize for ConOpts {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let creds_len = match self.login_kind() {
            LoginKind::Credentials(_) => 2,
            _ => 1,
        };

        let attr = HashMap::from([
            ("currentSchema", json!(self.schema().unwrap_or_default())),
            ("autocommit", json!(self.autocommit())),
            ("queryTimeout", json!(self.query_timeout())),
        ]);

        let mut opts = serializer.serialize_map(Some(FIXED_CON_OPTS_LEN + creds_len))?;

        match self.login_kind() {
            LoginKind::AccessToken(token) => opts.serialize_entry("accessToken", token)?,
            LoginKind::RefreshToken(token) => opts.serialize_entry("refreshToken", token)?,
            LoginKind::Credentials(cr) => {
                opts.serialize_entry("username", cr.username())?;
                opts.serialize_entry("password", cr.password())?;
            }
        }

        opts.serialize_entry("driverName", self.client_name.as_str())?;
        opts.serialize_entry("clientName", self.client_name.as_str())?;
        opts.serialize_entry("clientVersion", self.client_version.as_str())?;
        opts.serialize_entry("clientOs", self.client_os.as_str())?;
        opts.serialize_entry("clientRuntime", "Rust")?;
        opts.serialize_entry("useCompression", &self.compression())?;
        opts.serialize_entry("attributes", &attr)?;

        opts.end()
    }
}
