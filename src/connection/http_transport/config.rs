//! HTTP Transport options for IMPORT and EXPORT.
//!
//! Defaults to 0 threads (meaning a thread will be created for all available Exasol nodes in the cluster),
//! no compression while encryption is conditioned by the `native-tls` and `rustls` feature flags.

use crossbeam::channel::Sender;
use csv::Terminator;
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Barrier};
use std::time::Duration;

pub trait HttpTransportOpts {
    fn num_threads(&self) -> usize;

    fn encryption(&self) -> bool;

    fn compression(&self) -> bool;

    fn take_timeout(&mut self) -> Option<Duration>;

    /// Sets the timeout of socket read and write operations.
    /// The socket will error out of the timeout is exceeded.
    fn set_timeout(&mut self, timeout: Option<Duration>);
}

/// Export options
///
/// # Defaults
///
/// num_threads: 0 -> this means a thread per node will be spawned
/// compression: false
/// encryption: *if encryption features are enabled true, else false*
/// comment: None
/// encoding: None -> database default will be used
/// null: None -> by default NULL values turn to ""
/// row_separator: `csv` crate's special Terminator::CRLF
/// column_separator: ','
/// column_delimiter: '"'
/// timeout: 120 seconds
/// with_column_names: true
#[derive(Clone, Debug)]
pub struct ExportOpts {
    num_threads: usize,
    compression: bool,
    encryption: bool,
    query: Option<String>,
    table_name: Option<String>,
    comment: Option<String>,
    encoding: Option<String>,
    null: Option<String>,
    row_separator: Terminator,
    column_separator: u8,
    column_delimiter: u8,
    timeout: Option<Duration>,
    with_column_names: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for ExportOpts {
    fn default() -> Self {
        Self {
            num_threads: 0,
            compression: false,
            encryption: cfg!(any(feature = "native-tls-basic", feature = "rustls")),
            query: None,
            table_name: None,
            comment: None,
            encoding: None,
            null: None,
            row_separator: Terminator::CRLF,
            column_separator: b',',
            column_delimiter: b'"',
            timeout: Some(Duration::from_secs(120)),
            with_column_names: true,
        }
    }
}

impl HttpTransportOpts for ExportOpts {
    fn num_threads(&self) -> usize {
        self.num_threads
    }

    fn encryption(&self) -> bool {
        self.encryption
    }

    fn compression(&self) -> bool {
        self.compression
    }

    fn take_timeout(&mut self) -> Option<Duration> {
        self.timeout.take()
    }

    fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout
    }
}

impl ExportOpts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_encryption(&mut self, flag: bool) {
        Self::validate_encryption(flag);
        self.encryption = flag
    }

    pub fn set_compression(&mut self, flag: bool) {
        Self::validate_compression(flag);
        self.compression = flag
    }

    pub fn set_num_threads(&mut self, num: usize) {
        self.num_threads = num
    }

    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// Setting the query clears the table name
    pub fn set_query<T>(&mut self, query: T)
    where
        T: Into<String>,
    {
        self.query = Some(query.into());
        self.table_name = None;
    }

    pub fn table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }

    /// Setting the table name clears the query
    pub fn set_table_name<T>(&mut self, table: T)
    where
        T: Into<String>,
    {
        self.table_name = Some(table.into());
        self.query = None;
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn set_comment<T>(&mut self, comment: T)
    where
        T: Into<String>,
    {
        self.comment = Some(comment.into())
    }

    pub fn encoding(&self) -> Option<&str> {
        self.encoding.as_deref()
    }

    pub fn set_encoding<T>(&mut self, encoding: T)
    where
        T: Into<String>,
    {
        self.encoding = Some(encoding.into())
    }

    pub fn null(&self) -> Option<&str> {
        self.null.as_deref()
    }

    pub fn set_null<T>(&mut self, value: T)
    where
        T: Into<String>,
    {
        self.null = Some(value.into())
    }

    pub fn row_separator(&self) -> Terminator {
        self.row_separator
    }

    pub fn set_row_separator(&mut self, sep: Terminator) {
        self.row_separator = sep
    }

    pub fn column_separator(&self) -> u8 {
        self.column_separator
    }

    pub fn set_column_separator(&mut self, sep: u8) {
        self.column_separator = sep
    }

    pub fn column_delimiter(&self) -> u8 {
        self.column_delimiter
    }

    pub fn set_column_delimiter(&mut self, delimiter: u8) {
        self.column_delimiter = delimiter
    }

    pub fn with_column_names(&self) -> bool {
        self.with_column_names
    }

    /// When this is `true`, which is the default, the column names header is also exported
    /// as the first row. This is important for deserializing structs from rows, for instance.
    pub fn set_with_column_names(&mut self, flag: bool) {
        self.with_column_names = flag
    }

    fn validate_encryption(flag: bool) {
        if flag && cfg!(not(any(feature = "native-tls-basic", feature = "rustls"))) {
            panic!("native-tls or rustls features must be enabled to use encryption")
        }
    }

    fn validate_compression(flag: bool) {
        if flag && cfg!(not(feature = "flate2")) {
            panic!("flate2 feature must be enabled to use compression")
        }
    }
}

/// HTTP Transport import options.
///
/// # Defaults
///
/// num_threads: 0 -> this means a thread per node will be spawned
/// compression: false
/// encryption: *if encryption features are enabled true, else false*
/// columns: None -> all table columns will be considered
/// comment: None
/// encoding: None -> database default will be used
/// null: None -> by default NULL values turn to ""
/// row_separator: `csv` crate's special Terminator::CRLF
/// column_separator: ','
/// column_delimiter: '"'
/// timeout: 120 seconds
/// skip: 0 rows
/// trim: None
#[derive(Clone, Debug)]
pub struct ImportOpts {
    num_threads: usize,
    compression: bool,
    encryption: bool,
    columns: Option<Vec<String>>,
    table_name: Option<String>,
    comment: Option<String>,
    encoding: Option<String>,
    null: Option<String>,
    row_separator: Terminator,
    column_separator: u8,
    column_delimiter: u8,
    timeout: Option<Duration>,
    skip: usize,
    trim: Option<TrimType>,
}

#[allow(clippy::derivable_impls)]
impl Default for ImportOpts {
    fn default() -> Self {
        Self {
            num_threads: 0,
            compression: false,
            encryption: cfg!(any(feature = "native-tls-basic", feature = "rustls")),
            columns: None,
            table_name: None,
            comment: None,
            encoding: None,
            null: None,
            row_separator: Terminator::CRLF,
            column_separator: b',',
            column_delimiter: b'"',
            timeout: Some(Duration::from_secs(120)),
            skip: 0,
            trim: None,
        }
    }
}

impl HttpTransportOpts for ImportOpts {
    fn num_threads(&self) -> usize {
        self.num_threads
    }

    fn encryption(&self) -> bool {
        self.encryption
    }

    fn compression(&self) -> bool {
        self.compression
    }

    fn take_timeout(&mut self) -> Option<Duration> {
        self.timeout.take()
    }

    fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout
    }
}

impl ImportOpts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_encryption(&mut self, flag: bool) {
        Self::validate_encryption(flag);
        self.encryption = flag
    }

    pub fn set_compression(&mut self, flag: bool) {
        Self::validate_compression(flag);
        self.compression = flag
    }

    pub fn set_num_threads(&mut self, num: usize) {
        self.num_threads = num
    }

    pub fn columns(&self) -> Option<&[String]> {
        self.columns.as_deref()
    }

    pub fn set_columns<I, T>(&mut self, columns: I)
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.columns = Some(
            columns
                .into_iter()
                .map(|s| s.into())
                .collect::<Vec<String>>(),
        );
    }

    pub fn table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }

    pub fn set_table_name<T>(&mut self, table: T)
    where
        T: Into<String>,
    {
        self.table_name = Some(table.into());
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn set_comment<T>(&mut self, comment: T)
    where
        T: Into<String>,
    {
        self.comment = Some(comment.into())
    }

    pub fn encoding(&self) -> Option<&str> {
        self.encoding.as_deref()
    }

    pub fn set_encoding<T>(&mut self, encoding: T)
    where
        T: Into<String>,
    {
        self.encoding = Some(encoding.into())
    }

    pub fn null(&self) -> Option<&str> {
        self.null.as_deref()
    }

    pub fn set_null<T>(&mut self, value: T)
    where
        T: Into<String>,
    {
        self.null = Some(value.into())
    }

    pub fn row_separator(&self) -> Terminator {
        self.row_separator
    }

    pub fn set_row_separator(&mut self, sep: Terminator) {
        self.row_separator = sep
    }

    pub fn column_separator(&self) -> u8 {
        self.column_separator
    }

    pub fn set_column_separator(&mut self, sep: u8) {
        self.column_separator = sep
    }

    pub fn column_delimiter(&self) -> u8 {
        self.column_delimiter
    }

    pub fn set_column_delimiter(&mut self, delimiter: u8) {
        self.column_delimiter = delimiter
    }

    pub fn skip(&self) -> usize {
        self.skip
    }

    /// Skipping rows could be used for skipping the header row of a file, for instance.
    pub fn set_skip(&mut self, num: usize) {
        self.skip = num
    }

    pub fn trim(&self) -> Option<TrimType> {
        self.trim
    }

    pub fn set_trim(&mut self, trim: Option<TrimType>) {
        self.trim = trim
    }

    fn validate_encryption(flag: bool) {
        if flag && cfg!(not(any(feature = "native-tls-basic", feature = "rustls"))) {
            panic!("native-tls or rustls features must be enabled to use encryption")
        }
    }

    fn validate_compression(flag: bool) {
        if flag && cfg!(not(feature = "flate2")) {
            panic!("flate2 feature must be enabled to use compression")
        }
    }
}

/// Trim options for IMPORT
#[derive(Debug, Clone, Copy)]
pub enum TrimType {
    Left,
    Right,
    Both,
}

impl Display for TrimType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Left => write!(f, "LTRIM"),
            Self::Right => write!(f, "RTRIM"),
            Self::Both => write!(f, "TRIM"),
        }
    }
}

/// Struct that holds internal utilities and parameters for HTTP transport
#[derive(Clone, Debug)]
pub struct HttpTransportConfig {
    pub barrier: Arc<Barrier>,
    pub run: Arc<AtomicBool>,
    pub addr_sender: Sender<String>,
    pub server_addr: String,
    pub encryption: bool,
    pub compression: bool,
    pub timeout: Option<Duration>,
}

impl HttpTransportConfig {
    /// Generates a Vec of configs, one for each given address
    pub fn generate(
        hosts: Vec<String>,
        barrier: Arc<Barrier>,
        run: Arc<AtomicBool>,
        addr_sender: Sender<String>,
        use_encryption: bool,
        use_compression: bool,
        timeout: Option<Duration>,
    ) -> Vec<Self> {
        hosts
            .into_iter()
            .map(|server_addr| Self {
                server_addr,
                barrier: barrier.clone(),
                run: run.clone(),
                addr_sender: addr_sender.clone(),
                encryption: use_encryption,
                compression: use_compression,
                timeout,
            })
            .collect()
    }

    pub fn take_timeout(&mut self) -> Option<Duration> {
        self.timeout.take()
    }
}
