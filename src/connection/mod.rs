use crate::con_opts::{ConOpts, Credentials, LoginKind};
use crate::connection::http_transport::{ExaReader, ExaWriter};
use crate::error::{ConnectionError, DriverError, Error, HttpTransportError};
use crate::error::{QueryError, RequestError, Result};
use csv::{Reader, StringRecord, Terminator, WriterBuilder};
use driver_attr::DriverAttributes;
pub use http_transport::{ExportOpts, HttpTransportOpts, ImportOpts, TrimType};
use http_transport::{HttpExportJob, HttpImportJob, HttpTransportJob};
use lazy_regex::regex;
use log::{error, info};
use regex::Captures;
use response::{Attributes, FetchedData, QueryResultDe, Response, ResponseData, ResultSetDe};
pub use response::{Column, DataType, ExaError, PreparedStatement};
use result::ResultSetIter;
pub use result::{QueryResult, ResultSet};
pub use row::deserialize_as_seq;
use row::{to_col_major, Row};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::fs::OpenOptions;
use std::iter::Flatten;
use std::net::TcpStream;
use std::ops::ControlFlow;
use std::option::IntoIter;
use std::path::Path;
use tls_cert::Certificate;
#[cfg(any(feature = "rustls", feature = "native-tls-basic"))]
use tungstenite::client_tls_with_config;
#[cfg(any(feature = "rustls", feature = "native-tls-basic"))]
pub use tungstenite::Connector;
use tungstenite::{stream::MaybeTlsStream, Message, WebSocket};
use ws::ExaWebSocket;

mod driver_attr;
mod http_transport;
mod response;
mod result;
mod row;
mod tls_cert;
mod ws;

const TRANSPORT_BUFFER_SIZE: usize = 65536;

// Convenience aliases
type ReqResult<T> = std::result::Result<T, RequestError>;
type ConResult<T> = std::result::Result<T, ConnectionError>;
type Ws = WebSocket<MaybeTlsStream<TcpStream>>;
type WsAddr = (Ws, String, u16);
type WsParts = (WsAddr, Option<String>);

/// Convenience function to quickly connect using default options.
/// Returns a [Connection] set using the default [ConOpts]
/// ```
/// use exasol::connect;
/// use std::env;
///
/// let dsn = env::var("EXA_DSN").unwrap();
/// let schema = env::var("EXA_SCHEMA").unwrap();
/// let user = env::var("EXA_USER").unwrap();
/// let password = env::var("EXA_PASSWORD").unwrap();
///
/// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
/// ```
pub fn connect<T>(dsn: T, schema: T, user: T, password: T) -> Result<Connection>
where
    T: Into<String>,
{
    let mut opts = ConOpts::new();
    opts.set_dsn(dsn);
    opts.set_schema(Some(schema));
    let login_kind = LoginKind::Credentials(Credentials::new(user, password));
    opts.set_login_kind(login_kind);

    Connection::new(opts)
}

/// The [Connection] struct will be what we use to interact with the database.
/// The connection keeps track of all the result sets and prepared statements issued, and,
/// if they are not closed by the user, they will automatically get closed when the
/// connection is dropped.
///
/// As a best practice, though, put some effort into manually closing the results and
/// prepared statements created so as not to bloat the connection.
pub struct Connection {
    driver_attr: DriverAttributes,
    ws: ExaWebSocket,
    rs_handles: HashSet<u16>,
    ps_handles: HashSet<u16>,
}

impl Drop for Connection {
    /// Implementing drop to properly get rid of the connection and its components
    fn drop(&mut self) {
        // Closes result sets and prepared statements
        let rs_handles = std::mem::take(&mut self.rs_handles);
        if !rs_handles.is_empty() {
            self.close_results_impl(&rs_handles).ok();
        }

        std::mem::take(&mut self.ps_handles)
            .into_iter()
            .for_each(|h| drop(self.close_prepared_stmt_impl(h)));

        // Closing session on Exasol side
        self.do_request(json!({"command": "disconnect"})).ok();

        // Sending Message::Close frame
        self.ws.close().ok();

        // Reading the response sent by the server until Error:ConnectionClosed occurs.
        // We should typically get a Message::Close frame and then the error.
        while self.ws.read_message().is_ok() {}

        // It is now safe to drop the socket
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str_attr = self
            .ws
            .exa_attr()
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect::<Vec<String>>()
            .join("\n");
        write!(
            f,
            "active: {}\n{}\n{}",
            self.ws.can_write(),
            str_attr,
            self.driver_attr
        )
    }
}

impl Connection {
    /// Creates a [Connection] using the provided [ConOpts].
    /// If a range is provided as DSN, connection attempts are made for max each possible
    /// options, until one is successful.
    ///
    /// # Errors
    /// If all options were exhausted and a connection could not be established
    /// an error is returned.
    /// ```
    /// use std::env;
    /// use exasol::*;
    ///
    /// let dsn = env::var("EXA_DSN").unwrap();
    /// let schema = env::var("EXA_SCHEMA").unwrap();
    /// let user = env::var("EXA_USER").unwrap();
    /// let password = env::var("EXA_PASSWORD").unwrap();
    ///
    /// let mut opts = ConOpts::new();
    /// opts.set_dsn(dsn);
    /// opts.set_login_kind(LoginKind::Credentials(Credentials::new(user, password)));
    /// opts.set_schema(None::<String>);
    ///
    /// let mut exa_con = Connection::new(opts).unwrap();
    /// ```
    pub fn new(opts: ConOpts) -> Result<Connection> {
        let cb = |prefix: &str, addr: &str, port: u16| {
            let url = format!("{}://{}:{}/", prefix, addr, port);
            let (ws, _) = tungstenite::connect(url)?;
            Ok(ws)
        };

        Self::create(cb, opts)
    }

    /// Creates a [Connection] using a custom [Connector] and a set of [ConOpts].
    ///
    /// This method is only available when either the `rustls` or `native-tls` features
    /// are enabled. Note that encryption in the [ConOpts] will always be automatically
    /// enabled by this method.
    ///
    /// This method gives more control over how the TLS connection is established,
    /// e.g: you would use this if you want to rely on a self-signed certificate,
    /// or a custom root store.
    #[cfg(any(feature = "native-tls-basic", feature = "rustls"))]
    pub fn from_connector(connector: Connector, mut opts: ConOpts) -> Result<Connection> {
        // Cloning the connector is necessary because we'll call this closure
        // once for each IP resolved from the DSN, in case a range was provided.
        let cb = |prefix: &str, addr: &str, port: u16| {
            let ws_addr = format!("{}://{}:{}/", prefix, addr, port);
            let stream = TcpStream::connect(format!("{}:{}", addr, port))?;
            // Manually cloning because Connector does not implement Clone.
            let connector = match &connector {
                #[cfg(feature = "native-tls-basic")]
                Connector::NativeTls(con) => Connector::NativeTls(con.clone()),

                #[cfg(feature = "rustls")]
                Connector::Rustls(con) => Connector::Rustls(con.clone()),
                _ => Connector::Plain,
            };
            let (ws, _) = client_tls_with_config(ws_addr, stream, None, Some(connector))?;
            Ok(ws)
        };

        opts.set_encryption(true);
        Self::create(cb, opts)
    }

    /// Returns the fetch size of [ResultSet] chunks
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(dsn, schema, user, password).unwrap();
    /// // Size is in bytes
    /// assert_eq!(exa_con.fetch_size(), 5 * 1024 * 1024);
    /// ```
    #[inline]
    pub fn fetch_size(&self) -> usize {
        self.driver_attr.fetch_size
    }

    /// Sets the fetch size in bytes when retrieving [ResultSet] chunks.
    ///
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// // Size is in bytes
    /// exa_con.set_fetch_size(2 * 1024 * 1024);
    /// ```
    #[inline]
    pub fn set_fetch_size(&mut self, val: usize) {
        self.driver_attr.fetch_size = val;
    }

    /// Returns whether the result set columns are implicitly converted to lower case
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// assert_eq!(exa_con.lowercase_columns(), true);
    /// ```
    #[inline]
    pub fn lowercase_columns(&self) -> bool {
        self.driver_attr.lowercase_columns
    }

    /// Sets whether the [ResultSet] [Column] names will be implicitly cast to lowercase.
    ///
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.set_lowercase_columns(false);
    /// ```
    #[inline]
    pub fn set_lowercase_columns(&mut self, flag: bool) {
        self.driver_attr.lowercase_columns = flag;
    }

    /// Returns whether autocommit is enabled
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// assert_eq!(exa_con.autocommit(), true);
    /// ```
    // This should not ever fail to unwrap but
    // we'll return the default set by the driver just in case
    // An alternative would maybe be setting it if it does not exist,
    // but there's clearly some problem if that happens as it's always
    // in the ConOpts.
    #[inline]
    pub fn autocommit(&self) -> bool {
        self.ws
            .exa_attr()
            .get("autocommit")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
    }

    /// Sets autocommit mode On or Off. The default is On.
    /// Turning it off means transaction mode is enabled and explicit `COMMIT` or `ROLLBACK`
    /// statements have to be executed.
    ///
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.set_autocommit(false).unwrap();
    /// ```
    #[inline]
    pub fn set_autocommit(&mut self, val: bool) -> Result<()> {
        let payload = json!({ "autocommit": val });
        self.set_attributes(payload)
    }

    /// Returns the current query timeout
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// assert_eq!(exa_con.query_timeout(), 0);
    /// ```
    // This should not ever fail to unwrap but
    // we'll return the default set by the driver just in case
    // An alternative would maybe be setting it if it does not exist,
    // but there's clearly some problem if that happens as it's always
    // in the ConOpts.
    #[inline]
    pub fn query_timeout(&self) -> u64 {
        self.ws
            .exa_attr()
            .get("queryTimeout")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    }

    /// Sets the query timeout
    ///
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.set_query_timeout(60).unwrap();
    /// ```
    #[inline]
    pub fn set_query_timeout(&mut self, val: u64) -> Result<()> {
        let payload = json!({ "queryTimeout": val });
        self.set_attributes(payload)
    }

    /// Returns the currently open schema, if any
    /// This can be NULL, hence an Option is returned
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// assert_eq!(exa_con.schema(), Some(schema.as_str()));
    /// ```
    #[inline]
    pub fn schema(&self) -> Option<&str> {
        self.ws
            .exa_attr()
            .get("currentSchema")
            .and_then(|v| v.as_str())
    }

    /// Sets the currently open schema
    ///
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.set_schema(Some(schema)).unwrap();
    /// ```
    #[inline]
    pub fn set_schema<T>(&mut self, schema: Option<T>) -> Result<()>
    where
        T: AsRef<str> + Serialize,
    {
        let payload = json!({ "currentSchema": schema });
        self.set_attributes(payload)
    }

    /// Sends a query to the database and waits for the result.
    /// Returns a [QueryResult]
    ///
    /// ```
    /// # use exasol::{connect, QueryResult};
    /// # use exasol::error::Result;
    /// # use serde_json::Value;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let result = exa_con.execute("SELECT 1, 2 UNION ALL SELECT 1, 2;").unwrap();
    /// ```
    pub fn execute<T>(&mut self, query: T) -> Result<QueryResult>
    where
        T: AsRef<str> + Serialize,
    {
        let payload = json!({"command": "execute", "sqlText": query});
        self.exec_with_one_result(payload)
            .map_err(|e| QueryError::map_err(e, &query))
    }

    /// Sends multiple queries to the database and waits for their results.
    /// Returns a [Vec<QueryResult>].
    ///
    /// ```
    /// # use exasol::{connect, QueryResult};
    /// # use exasol::error::Result;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let queries = vec!["SELECT 3", "SELECT 4"];
    /// let results: Vec<QueryResult> = exa_con.execute_batch(&queries).unwrap();
    /// let queries = vec!["SELECT 3", "DELETE * FROM EXA_RUST_TEST WHERE 1 = 2"];
    /// let results: Vec<QueryResult> = exa_con.execute_batch(&queries).unwrap();
    /// ```
    pub fn execute_batch<T>(&mut self, queries: &[T]) -> Result<Vec<QueryResult>>
    where
        T: AsRef<str> + Serialize,
    {
        let payload = json!({"command": "executeBatch", "sqlTexts": queries});
        self.exec_with_results(payload)
    }

    /// For a given mutable reference of a [QueryResult],
    /// returns an iterator over the underlying [ResultSet], if there's one.
    /// If there is no [ResultSet], the iterator immediately returns `None`.
    /// Otherwise, rows are returned as `Some(Result<T>)` where T is the type
    /// the rows will get deserialized into.
    ///
    /// Given an iterator here, the full toolset of [Iterator] can be used. The [QueryResult],
    /// or rather the [ResultSet] in it, holds an internal state of the result set iteration.
    ///
    /// # Important
    /// The iterator holds a mutable reference to the [Connection], to be able to
    /// retrieve rows as needed, as the iterator itself is lazy and data is retrieved in chunks.
    ///
    /// This is important because it means there can't be two result sets coming from the
    /// same connection getting iterated over at the same time, or interleaved in any way.
    /// You have to work with one result set at a time, collecting/storing it locally
    /// and then generate a new iterator for a different [QueryResult].
    ///
    /// The mutable reference approach, compared to a previous interior mutability implementation,
    /// is both indirection free, thus faster, and Send + Sync, meaning the [Iterator] resulted
    /// can even be piped into `rayon::par_bridge()` seamlessly.
    ///
    /// You are not, however, restricted to iterating over the entire result set at a given time.
    /// Since the iterator is generated from a mutable reference of the [QueryResult], you can,
    /// say, get the iterator, `take` only the first 100 rows, drop the iterator, but sometime
    /// later, generate a new iterator from the same [QueryResult] and collect the rest of the
    /// result set.
    ///
    ///
    /// The rule is that if there are less than 1000 rows in the result set, they are
    /// returned immediately. Otherwise, the data is retrieved in chunks from the database
    /// based on the `fetch_size` attribute of the connection.
    /// ```
    /// # use exasol::error::Result;
    /// # use exasol::{connect, bind, QueryResult};
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let mut result = exa_con.execute("SELECT '1', '2', '3' UNION ALL SELECT '4', '5', '6'").unwrap();
    /// let data = exa_con.iter_result(&mut result).collect::<Result<Vec<Vec<String>>>>().unwrap();
    /// assert_eq!(data.len(), 2);
    /// ```
    pub fn iter_result<'a, T: DeserializeOwned>(
        &'a mut self,
        result: &'a mut QueryResult,
    ) -> Flatten<IntoIter<ResultSetIter<'a, T>>> {
        let iter = result
            .result_set_mut()
            .map(|rs| ResultSetIter::new(rs, self));
        iter.into_iter().flatten()
    }

    /// HTTP Transport export with a closure that deserializes rows into a `Vec`.
    /// ```
    /// # use exasol::{connect, ExportOpts, QueryResult};
    /// # use exasol::error::Result;
    /// # use serde_json::Value;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let mut opts = ExportOpts::new();
    /// opts.set_query("SELECT 'a', 'b', 1 UNION ALL SELECT 'c', 'd', 2;");
    /// let result = exa_con.export_to_vec(opts).unwrap();
    ///
    /// result.into_iter().take(5).for_each(|v: (String, String, u32)| println!("{:?}", v))
    /// ```
    pub fn export_to_vec<T>(&mut self, mut opts: ExportOpts) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        // Overwriting these values as that's what the CSV reader expects.
        opts.set_row_separator(Terminator::CRLF);
        opts.set_column_separator(b',');
        opts.set_column_delimiter(b'"');
        opts.set_null("");
        opts.set_with_column_names(true);

        let closure = |reader: ExaReader| {
            let mut csv_reader = Reader::from_reader(reader);

            // Convert headers to lowercase to allow easier deserialization of fields
            let header_res = csv_reader.headers().map(|headers| {
                headers
                    .into_iter()
                    .map(|f| f.to_lowercase())
                    .collect::<StringRecord>()
            });

            // Remap headers into reader.
            let header_res = header_res.map(|headers| csv_reader.set_headers(headers));

            // Read and deserialize the data
            let data_res = header_res.and_then(|_| {
                csv_reader
                    .into_deserialize()
                    .collect::<csv::Result<Vec<T>>>()
            });

            map_csv_result(data_res)
        };

        self.export_to_closure(closure, opts).and_then(|r| r)
    }

    /// HTTP Transport export with a closure that creates and writes to a file.
    pub fn export_to_file<P>(&mut self, path: P, opts: ExportOpts) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let closure = |mut reader: ExaReader| {
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(path)
                .and_then(|mut file| std::io::copy(&mut reader, &mut file))
                .map_err(HttpTransportError::IoError)
                .map_err(DriverError::HttpTransportError)?;

            Ok(())
        };

        self.export_to_closure(closure, opts).and_then(|r| r)
    }

    /// HTTP Transport export implementation that can take any closure and an instance
    /// of [ExportOpts]. The closure must make use of a reader implementing [Read](std::io::Read).
    /// For examples check [Connection::export_to_file] and [Connection::export_to_vec].
    pub fn export_to_closure<F, T>(&mut self, callback: F, opts: ExportOpts) -> Result<T>
    where
        F: FnOnce(ExaReader) -> T,
    {
        opts.table_name()
            .or_else(|| opts.query())
            .ok_or(HttpTransportError::MissingParameter("table_name or query"))
            .map_err(DriverError::HttpTransportError)?;

        HttpExportJob::new(self, callback, opts).run()
    }

    /// HTTP Transport import with a closure that serializes rows from a type implementing
    /// [IntoIterator].
    /// ```
    /// # use exasol::*;
    /// # use exasol::error::Result;
    /// # use serde_json::Value;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let mut export_opts = ExportOpts::new();
    /// export_opts.set_query("SELECT 'a', 'b', 1 UNION ALL SELECT 'c', 'd', 2;");
    /// let result: Vec<(String, String, u32)> = exa_con.export_to_vec(export_opts).unwrap();
    /// let mut import_opts = ImportOpts::new();
    /// import_opts.set_table_name("EXA_RUST_TEST");
    /// exa_con.import_from_iter(result, import_opts).unwrap();
    /// ```
    pub fn import_from_iter<I, T>(&mut self, iter: I, mut opts: ImportOpts) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: Serialize,
    {
        // Overwriting these values as that's what the CSV writer expects.
        opts.set_row_separator(Terminator::CRLF);
        opts.set_column_separator(b',');
        opts.set_column_delimiter(b'"');
        opts.set_null("");

        let closure = |writer: ExaWriter| {
            let mut csv_writer = WriterBuilder::new()
                .has_headers(false)
                .buffer_capacity(TRANSPORT_BUFFER_SIZE)
                .from_writer(writer);

            let res = iter.into_iter().fold(Ok(()), |_, item| {
                csv_writer.serialize(item)?;
                Ok(())
            });

            map_csv_result(res)
        };

        self.import_from_closure(closure, opts).and_then(|r| r)
    }

    /// HTTP Transport import with a closure that reads data directly from a file.
    pub fn import_from_file<P>(&mut self, path: P, opts: ImportOpts) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let closure = |mut writer: ExaWriter| {
            OpenOptions::new()
                .read(true)
                .open(path)
                .and_then(|mut file| std::io::copy(&mut file, &mut writer))
                .map_err(HttpTransportError::IoError)
                .map_err(DriverError::HttpTransportError)?;

            Ok(())
        };

        self.import_from_closure(closure, opts).and_then(|r| r)
    }

    /// HTTP Transport import implementation that can take any closure and an instance
    /// of [ImportOpts]. The closure must make use of a writer implementing [Write](std::io::Write).
    /// For examples check [Connection::import_from_file] and [Connection::import_from_iter].
    pub fn import_from_closure<F, T>(&mut self, callback: F, opts: ImportOpts) -> Result<T>
    where
        F: FnOnce(ExaWriter) -> T,
    {
        opts.table_name()
            .ok_or(HttpTransportError::MissingParameter("table_name"))
            .map_err(DriverError::HttpTransportError)?;

        HttpImportJob::new(self, callback, opts).run()
    }

    /// Creates a prepared statement of type [PreparedStatement].
    /// The prepared statement can then be executed with the provided data.
    ///
    /// Named parameters are supported to aid in using map-like types as data rows
    /// when executing the prepared statement. Using just `?` results in the parameter name
    /// being the empty string.
    ///
    /// Since a map-like type implies no duplicate keys, duplicate named parameters
    /// are not supported and will result in errors when the [PreparedStatement] is executed.
    ///
    /// For sequence-like types, parameter names are ignored and discarded.
    ///
    /// ```
    /// use exasol::{connect, QueryResult, PreparedStatement};
    /// use exasol::error::Result;
    /// use serde_json::json;
    /// use std::env;
    ///
    /// let dsn = env::var("EXA_DSN").unwrap();
    /// let schema = env::var("EXA_SCHEMA").unwrap();
    /// let user = env::var("EXA_USER").unwrap();
    /// let password = env::var("EXA_PASSWORD").unwrap();
    ///
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let prepared_stmt = exa_con.prepare("SELECT 1 FROM (SELECT 1) TMP WHERE 1 = ?").unwrap();
    ///
    /// let data = vec![vec![json!(1)]];
    /// exa_con.execute_prepared(&prepared_stmt, &data).unwrap();
    ///
    /// // Prepared statements should be closed once you're done with them:
    /// exa_con.close_prepared_statement(prepared_stmt).unwrap();
    /// ```
    ///
    /// ```
    /// use exasol::{connect, QueryResult, PreparedStatement};
    /// use exasol::error::Result;
    /// use serde_json::json;
    /// use serde::Serialize;
    /// use std::env;
    ///
    /// let dsn = env::var("EXA_DSN").unwrap();
    /// let schema = env::var("EXA_SCHEMA").unwrap();
    /// let user = env::var("EXA_USER").unwrap();
    /// let password = env::var("EXA_PASSWORD").unwrap();
    ///
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let prepared_stmt = exa_con.prepare("INSERT INTO EXA_RUST_TEST VALUES(?col1, ?col2, ?col3)").unwrap();
    ///
    /// #[derive(Serialize, Clone)]
    /// struct Data {
    ///    col1: String,
    ///    col2: String,
    ///    col3: u8
    /// }
    ///
    /// let data_item = Data { col1: "t".to_owned(), col2: "y".to_owned(), col3: 10 };
    /// let vec_data = vec![data_item.clone(), data_item.clone(), data_item];
    ///
    /// exa_con.execute_prepared(&prepared_stmt, vec_data).unwrap();
    /// ```
    ///
    /// String literals resembling a parameter can be escaped, if needed:
    ///
    /// ```
    /// # use exasol::{connect, QueryResult, PreparedStatement};
    /// # use exasol::error::Result;
    /// # use serde_json::json;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let prepared_stmt = exa_con.prepare("INSERT INTO EXA_RUST_TEST VALUES('\\?col1', ?col2, ?col3)").unwrap();
    /// let data = vec![json!(["test", 1])];
    /// exa_con.execute_prepared(&prepared_stmt, &data).unwrap();
    /// ```
    pub fn prepare<T>(&mut self, query: T) -> Result<PreparedStatement>
    where
        T: Serialize + AsRef<str>,
    {
        let re = regex!(r"\\(\?\w*)|[?\w]\?\w*|\?\w*\?|\?(\w*)");
        let mut col_names = Vec::new();
        let str_query = query.as_ref();

        // This is similar to parameter binding, but uses ? instead of :
        //
        // Capture group 2 is Some when an actual parameter name is matched,
        // in which case it needs to be stored as a column name.
        // A simple question mark is returned to replace_all(),
        // as this is just the driver's mechanism for accepting named parameters
        // and Exasol has no clue about it.
        //
        // Capture group 1 is Some only when an escaped parameter construct
        // is matched(e.g: "\?PARAM"). Returning the group gets rid of the escape question mark.
        //
        // Otherwise, capture group 0, AKA the entire match, is returned as-is,
        // as it represents a regex match that we purposely ignore.
        // It's safe to unwrap it because it wouldn't be there if there is no match.
        let q = re.replace_all(str_query, |cap: &Captures| {
            cap.get(2)
                .map(|m| {
                    col_names.push(m.as_str().to_owned());
                    "?"
                })
                .or_else(|| cap.get(1).map(|m| &str_query[m.range()]))
                .unwrap_or(&str_query[cap.get(0).unwrap().range()])
        });

        let payload = json!({"command": "createPreparedStatement", "sqlText": q});
        let mut ps: PreparedStatement = self
            .get_resp_data(payload)
            .map_err(|e| QueryError::map_err(e, &query))?
            .try_into()?;
        ps.update_param_names(col_names);
        self.ps_handles.insert(ps.handle());
        Ok(ps)
    }

    /// Executes the prepared statement with the given data.
    /// Data must implement [IntoIterator] where `Item` implements [Serialize].
    /// Each `Item` of the iterator will represent a data row.
    ///
    /// If `Item` is map-like, the needed columns are retrieved and consumed,
    /// getting reordered based on the expected order given through the named parameters.
    ///
    /// If `Item` is sequence-like, the data is used as-is.
    /// Parameter names are ignored.
    ///
    /// # Errors
    ///
    /// Missing parameter names in map-like types (which can also be caused by duplication)
    /// or too few/many columns in sequence-like types results in errors.
    ///
    /// ```
    /// # use exasol::{connect, QueryResult};
    /// # use exasol::error::Result;
    /// # use serde_json::Value;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// use serde_json::json;
    /// use serde::Serialize;
    ///
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let prep_stmt = exa_con.prepare("INSERT INTO EXA_RUST_TEST VALUES(?col1, ?col2, ?col3)").unwrap();
    ///
    /// let json_data = json!(
    ///     [
    ///         ["a", "b", 1],
    ///         ["c", "d", 2],
    ///         ["e", "f", 3],
    ///         ["g", "h", 4]
    ///     ]
    /// );
    ///
    /// exa_con.execute_prepared(&prep_stmt, json_data.as_array().unwrap()).unwrap();
    ///
    /// #[derive(Serialize, Clone)]
    /// struct Data {
    ///    col1: String,
    ///    col2: String,
    ///    col3: u8
    /// }
    ///
    /// let data_item = Data { col1: "t".to_owned(), col2: "y".to_owned(), col3: 10 };
    /// let vec_data = vec![data_item.clone(), data_item.clone(), data_item];
    ///
    /// exa_con.execute_prepared(&prep_stmt, vec_data).unwrap();
    /// ```
    pub fn execute_prepared<T, S>(&mut self, ps: &PreparedStatement, data: T) -> Result<QueryResult>
    where
        S: Serialize,
        T: IntoIterator<Item = S>,
    {
        let (num_columns, columns) = ps
            .params()
            .map(|p| (p.num_columns(), p.columns()))
            .unwrap_or((0, [].as_slice()));

        let col_names = columns.iter().map(|c| c.name()).collect::<Vec<_>>();
        let col_major_data = to_col_major(&col_names, data).map_err(DriverError::DataError)?;
        let num_rows = col_major_data.num_rows();

        let payload = json!({
            "command": "executePreparedStatement",
            "statementHandle": ps.handle(),
            "numColumns": num_columns,
            "numRows": num_rows,
            "columns": columns,
            "data": col_major_data
        });

        self.exec_with_one_result(payload)
    }

    /// Consumes this [QueryResult], closing the inner [ResultSet], if there's one
    /// and it was not already closed.
    /// ```
    /// # use exasol::error::Result;
    /// # use exasol::{connect, bind, QueryResult};
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let mut result = exa_con.execute("SELECT '1', '2', '3' UNION ALL SELECT '4', '5', '6'").unwrap();
    /// exa_con.close_result(result);
    /// ```
    pub fn close_result(&mut self, qr: QueryResult) -> Result<()> {
        qr.result_set()
            .and_then(|rs| match rs.is_closed() {
                true => None,
                false => rs.handle(),
            })
            .map(|h| self.close_results_impl(&[h]))
            .unwrap_or(Ok(()))
    }

    /// Consumes this [PreparedStatement], closing it.
    /// ```
    /// # use exasol::{connect, QueryResult};
    /// # use exasol::error::Result;
    /// # use serde_json::Value;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// use serde_json::json;
    /// use serde::Serialize;
    ///
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let prep_stmt = exa_con.prepare("INSERT INTO EXA_RUST_TEST VALUES(?col1, ?col2, ?col3)").unwrap();
    /// exa_con.close_prepared_statement(prep_stmt);
    /// ```
    pub fn close_prepared_statement(&mut self, ps: PreparedStatement) -> Result<()> {
        self.close_prepared_stmt_impl(ps.handle())
    }

    /// Returns a Vec containing the addresses of
    /// all nodes in the Exasol cluster
    pub fn get_nodes(&mut self) -> Result<Vec<String>> {
        let addr = self.driver_attr.server_ip.as_str();
        let port = self.driver_attr.port;
        let payload = json!({"command": "getHosts", "hostIp": addr});
        let hosts: Vec<String> = self.get_resp_data(payload)?.try_into()?;

        Ok(hosts
            .into_iter()
            .map(|h| format!("{}:{}", h, port))
            .collect())
    }

    /// Ping the server and wait for a Pong frame
    ///
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.ping().unwrap();
    /// ```
    pub fn ping(&mut self) -> Result<()> {
        self.ws
            .write_message(Message::Ping(vec![]))
            .and(self.ws.read_message())
            .map_err(DriverError::RequestError)?;
        Ok(())
    }

    /// Closes multiple results sets by going over the result set handles [Iterator].
    #[inline]
    pub(crate) fn close_results_impl<I>(&mut self, handles: &I) -> Result<()>
    where
        for<'a> &'a I: IntoIterator<Item = &'a u16> + Serialize,
    {
        for h in handles {
            self.rs_handles.remove(h);
        }
        let payload = json!({"command": "closeResultSet", "resultSetHandles": handles});
        self.do_request(payload).map(|_| ())
    }

    /// Returns response data from a request.
    #[inline]
    pub(crate) fn get_resp_data(&mut self, payload: Value) -> Result<ResponseData> {
        self.ws.get_resp_data(payload)
    }

    fn create<F>(mut cb: F, opts: ConOpts) -> Result<Connection>
    where
        F: Fn(&str, &str, u16) -> ConResult<Ws>,
    {
        let parts = Self::ws_from_closure(&mut cb, &opts).map_err(DriverError::ConnectionError)?;
        let (ws_addr, fingerprint) = parts;
        let (ws, addr, port) = ws_addr;

        let driver_attr = DriverAttributes {
            port,
            server_ip: addr,
            fetch_size: opts.fetch_size(),
            lowercase_columns: opts.lowercase_columns(),
        };

        let mut ws = ExaWebSocket::new(ws, opts)?;

        fingerprint
            .map(|fp| ws.validate_fingerprint(fp))
            .unwrap_or(Ok(()))
            .map_err(DriverError::ConnectionError)?;

        let mut con = Self {
            rs_handles: HashSet::new(),
            ps_handles: HashSet::new(),
            driver_attr,
            ws,
        };

        // Get connection attributes from database
        con.get_attributes()?;
        Ok(con)
    }

    /// Sets connection attributes.
    #[inline]
    fn set_attributes(&mut self, attrs: Value) -> Result<()> {
        let payload = json!({"command": "setAttributes", "attributes": attrs});
        // Attributes have to be retrieved as well, for consistency.
        self.do_request(payload)
            .and_then(|_| self.get_attributes())
            .map(|_| ())
    }

    /// Closes a prepared statement through its handle.
    #[inline]
    fn close_prepared_stmt_impl(&mut self, h: u16) -> Result<()> {
        self.ps_handles.remove(&h);
        let payload = json!({"command": "closePreparedStatement", "statementHandle": h});
        self.do_request(payload).map(|_| ())
    }

    /// Sends a request for execution and returns the last [QueryResult] received.
    fn exec_with_one_result(&mut self, payload: Value) -> Result<QueryResult> {
        self.exec_with_results(payload)?
            .pop()
            .ok_or_else(|| DriverError::ResponseMismatch("result sets").into())
    }

    /// Gets the database results as [crate::query_result::Results]
    /// (that's what they deserialize into) and consumes them
    /// to return a usable vector of [QueryResult] enums.
    fn exec_with_results(&mut self, payload: Value) -> Result<Vec<QueryResult>> {
        let lc = self.driver_attr.lowercase_columns;
        let mut results: Vec<QueryResult> = self.get_resp_data(payload)?.try_into()?;

        results.iter_mut().for_each(|qr| {
            qr.lowercase_columns(lc);
            qr.result_set()
                .and_then(|rs| rs.handle())
                .map(|h| self.rs_handles.insert(h));
        });

        Ok(results)
    }

    /// This method attempts to create the websocket, authenticate in Exasol
    /// and read the connection attributes afterwards.
    ///
    /// We'll get the list of IP addresses resulted from parsing and resolving the DSN
    /// Then loop through all of them (max once each) until a connection is established.
    /// Login is attempted afterwards.
    fn ws_from_closure<F>(cb: &mut F, opts: &ConOpts) -> ConResult<WsParts>
    where
        F: Fn(&str, &str, u16) -> ConResult<Ws>,
    {
        let mut addresses = opts.parse_dsn()?;
        let fingerprint = addresses.take_fingerprint();
        let prefix = opts.ws_prefix();

        let cf = addresses.try_fold(ConnectionError::InvalidDSN, |_, (addr, port)| {
            Self::connect_ws(cb, prefix, addr, port)
        });

        match cf {
            ControlFlow::Break(ws) => Ok((ws, fingerprint)),
            ControlFlow::Continue(e) => Err(e),
        }
    }

    /// Attempts to create a websocket for the given URL
    /// Issues a Break variant if the connection was established
    /// or the Continue variant if an error was encountered.
    fn connect_ws<F>(
        cb: &mut F,
        prefix: &str,
        addr: String,
        port: u16,
    ) -> ControlFlow<WsAddr, ConnectionError>
    where
        F: Fn(&str, &str, u16) -> ConResult<Ws>,
    {
        info!(
            "Trying to connect websocket to {}://{}:{}",
            prefix, &addr, port
        );
        let res = cb(prefix, &addr, port);

        match res {
            Ok(ws) => {
                info!("Websocket connected!");
                ControlFlow::Break((ws, addr, port))
            }
            Err(e) => {
                error!("Could not connect websocket:\n {:#?}", &e);
                ControlFlow::Continue(e)
            }
        }
    }

    /// Gets connection attributes from Exasol
    #[inline]
    fn get_attributes(&mut self) -> Result<()> {
        let payload = json!({"command": "getAttributes"});
        self.do_request(payload)?;
        Ok(())
    }

    /// Sends a request and waits for its response.
    #[inline]
    fn do_request(&mut self, payload: Value) -> Result<Option<ResponseData>> {
        self.ws.do_request(payload)
    }
}

/// Convenience function for mapping a CSV Result to the crate's result type.
#[inline]
fn map_csv_result<T>(res: csv::Result<T>) -> Result<T> {
    res.map_err(HttpTransportError::CSVError)
        .map_err(DriverError::HttpTransportError)
        .map_err(Error::DriverError)
}
