//! # exasol
//!
//! Database connector for Exasol implemented using the Websocket protocol.
//! Messages are sent and received in the JSON format.
//!
//! # Errors
//!
//! Most, if not all, of the public API is implemented to return a [Result](crate::error::Result) due to
//! the nature of the library. Mentioning this here to avoid repeating the same note on all public functions.
//!
//! # Examples
//!
//! Using the [connect] function to conveniently create a connection.
//!
//! ```
//! use exasol::error::Result;
//! use exasol::{connect, bind, QueryResult};
//! use std::env;
//!
//! // Credentials can be retrieved however you like
//! let dsn = env::var("EXA_DSN").unwrap();
//! let schema = env::var("EXA_SCHEMA").unwrap();
//! let user = env::var("EXA_USER").unwrap();
//! let password = env::var("EXA_PASSWORD").unwrap();
//!
//! // Quick and convenient way to create a connection
//! let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
//!
//! // Executing a statement
//! let mut result = exa_con.execute("SELECT '1', '2', '3' UNION ALL SELECT '4', '5', '6'").unwrap();
//!
//! // Retrieving data associated with the result set.
//! // A Vec of rows is returned, and the row type in this case will be Vec<String>.
//! let data = exa_con.iter_result(&mut result).collect::<Result<Vec<Vec<String>>>>().unwrap();
//! ```
//!
//! The [Connection::iter_result] method returns a lazy iterator over a result's rows. Only a given
//! rows buffer is present at a given time in a [ResultSet], which is contained in a [QueryResult],
//! if the query had a result set. The buffer size can be changed either in the [ConOpts] or
//! through the [Connection::set_fetch_size] method.
//!
//! # Cleanup
//! [QueryResult] structs will automatically close once fully retrieved. If that does not happen,
//! they will behave like [PreparedStatement] structs in the sense that they should be manually
//! closed, or they will be closed when the [Connection] is dropped.
//!
//! # Best practice
//! As a best practice, you should always close [QueryResult] and [PreparedStatement] instances
//! once they are no longer needed.
//!
//! ```
//! use exasol::error::Result;
//! use exasol::{connect, bind, QueryResult, ResultSet};
//! use std::env;
//!
//! let dsn = env::var("EXA_DSN").unwrap();
//! let schema = env::var("EXA_SCHEMA").unwrap();
//! let user = env::var("EXA_USER").unwrap();
//! let password = env::var("EXA_PASSWORD").unwrap();
//!
//! let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
//! let mut result = exa_con.execute("SELECT * FROM EXA_RUST_TEST LIMIT 1000;").unwrap();
//! let mut counter = 0;
//!
//! while counter < 3 {
//!     // Only enough calls necessary to retrieve 100 rows will be made to the database.
//!     // Any leftover data in the chunk will still be present in the ResultSet buffer.
//!     // and can be used in later retrievals.
//!     let data: Vec<(String, String, u16)> = exa_con.iter_result(&mut result).take(100).collect::<Result<_>>().unwrap();
//!     // do stuff with data
//!
//!     counter += 1;
//! }
//!
//! // Alternatively, you can retrieve row chunks while there still are rows in the result set.
//! // while result.has_rows() {
//!     // let data: Vec<(String, String, u16)> = exa_con.fetch(&mut result, 100).unwrap();
//! // }
//!
//! // Failing to close the result here will leave it up to the
//! // Connection to do it when it is dropped.
//! exa_con.close_result(result);
//! ```
//!
//! # Custom Row Type
//!
//! The crate implements a custom deserializer, which is a trimmed down specialized version
//! of the `serde_json` deserializer. Database row deserialization can be attempted to any
//! type implementing [Deserialize](serde::Deserialize).
//!
//! This is thanks to the magic of `serde`. In addition, regular `serde` features are supported,
//! such as flattening or renaming. Note that implicitly, column names are converted to lowercase
//! for easier deserialization. This can be changed through the [Connection::set_lowercase_columns]
//! method.
//!
//! Map-like and sequence-like types are natively deserialized. For `enum` sequence-like variants,
//! see [deserialize_as_seq].
//!
//! ```
//! use exasol::{connect, QueryResult, ResultSet};
//! use exasol::error::Result;
//! use serde_json::Value;
//! use serde::Deserialize;
//! use std::env;
//!
//! let dsn = env::var("EXA_DSN").unwrap();
//! let schema = env::var("EXA_SCHEMA").unwrap();
//! let user = env::var("EXA_USER").unwrap();
//! let password = env::var("EXA_PASSWORD").unwrap();
//! let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
//! let mut result = exa_con.execute("SELECT 1, 2 UNION ALL SELECT 1, 2;").unwrap();
//!
//! // Change the expected row type with the turbofish notation
//! let mut data: Vec<[u8; 2]> = exa_con.iter_result(&mut result).collect::<Result<_>>().unwrap();
//! let row1 = data[0];
//!
//! // You can also rely on type inference.
//! // Nothing stops you from changing row types
//! // on the same result set.
//! let mut data: Vec<Vec<u8>> = exa_con.iter_result(&mut result).collect::<Result<_>>().unwrap();;
//! let row2 = data.pop();
//!
//! let mut result = exa_con.execute("SELECT 1 as col1, 2 as col2, 3 as col3 \
//!                               UNION ALL \
//!                               SELECT 4 as col1, 5 as col2, 6 as col3;").unwrap();
//!
//! #[derive(Debug, Deserialize)]
//! struct Test {
//!     col1: u8,
//!     col2: u8,
//!     col3: u8,
//! }
//!
//! let data = exa_con.iter_result::<Test>(&mut result).collect::<Result<Vec<_>>>().unwrap();
//! for row in data {
//!     // do stuff with row
//! }
//! ```
//!
//! # Parameter binding
//!
//! Queries can be composed by binding named or positional parameters to them through the [bind] function.
//! The function takes a string and a type implementing the [Serialize](serde::Serialize) trait.
//! The second argument must serialize to a sequence or map.
//!
//! Named parameter values behaviour:
//! * single value type get parsed to their SQL representation
//! * sequence-like types get parsed to a parenthesized list with elements in SQL representation
//! * map-like types get their values parsed to a parenthesized list with elements in SQL representation
//!
//! ```
//! use exasol::bind;
//! use serde::Serialize;
//!
//! #[derive(Serialize)]
//! struct Parameters {
//!     col1: String,
//!     col2: u16,
//!     col3: Vec<String>
//! }
//!
//! let params = Parameters {
//!     col1: "test".to_owned(),
//!     col2: 10,
//!     col3: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
//! };
//!
//! let query = "\
//!     SELECT * FROM TEST_TABLE \
//!     WHERE NAME = :col1 \
//!     AND ID = :col2 \
//!     AND VALUE IN :col3;";
//!
//! let new_query = bind(query, params).unwrap();
//! assert_eq!(new_query, "\
//!     SELECT * FROM TEST_TABLE \
//!     WHERE NAME = 'test' \
//!     AND ID = 10 \
//!     AND VALUE IN ('a', 'b', 'c');");
//! ```
//!
//! # Custom Connection
//!
//! The [Connection] struct can be directly instantiated with the use of [ConOpts].
//!
//! ```
//! use exasol::*;
//! use std::env;
//!
//! let dsn = env::var("EXA_DSN").unwrap();
//! let schema = env::var("EXA_SCHEMA").unwrap();
//! let user = env::var("EXA_USER").unwrap();
//! let password = env::var("EXA_PASSWORD").unwrap();
//!
//! // Only providing fields for which we want custom values
//! let mut opts = ConOpts::new();
//! opts.set_dsn(dsn);
//! opts.set_login_kind(LoginKind::Credentials(Credentials::new(user, password)));
//! opts.set_schema(Some(schema));
//!
//! let exa_con = Connection::new(opts).unwrap();
//! ```
//!
//! # Features
//!
//! - `native-tls` - (disabled by default) enables `tungstenite` WSS encryption support through native-tls
//! - `native-tls-vendored` - (disabled by default) enables `tungstenite` WSS encryption support through native-tls-vendored
//! - `rustls-tls-webpki-roots` - (disabled by default) enables `tungstenite` WSS encryption support through rustls-tls-webpki-roots
//! - `rustls-tls-native-roots` - (disabled by default) enables `tungstenite` WSS encryption support through rustls-tls-native-roots
//! - `flate2` - (disabled by default) enables compression support
//!
//! Enabling these features allows changing additional settings in [ConOpts] instances.
//!
//! # Panics
//! Attempting to use these methods without their respective features enabled results in panics.
//!
//! ``` should_panic
//! # use exasol::*;
//! # use std::env;
//! #
//! # let dsn = env::var("EXA_DSN").unwrap();
//! # let schema = env::var("EXA_SCHEMA").unwrap();
//! # let user = env::var("EXA_USER").unwrap();
//! # let password = env::var("EXA_PASSWORD").unwrap();
//! #
//! let mut opts = ConOpts::new();
//!
//! opts.set_dsn(dsn);
//! opts.set_login_kind(LoginKind::Credentials(Credentials::new(user, password)));
//! opts.set_schema(Some(schema));
//!
//! opts.set_encryption(true);
//! opts.set_compression(true);
//! ```
//!
//! # Batch Execution
//!
//! Batch query execution can be achieved through either
//! [Connection::execute_batch] or [PreparedStatement].
//!
//! # HTTP Transport
//!
//! Parallel, highly performant, IMPORT/EXPORT operations are supported through
//! [Connection::export_to_closure] and [Connection::import_from_closure].

mod con_opts;
mod connection;
pub mod error;
mod params;

pub use con_opts::{ConOpts, Credentials, LoginKind, ProtocolVersion};
#[cfg(any(feature = "rustls", feature = "native-tls-basic"))]
pub use connection::Connector;
pub use connection::{connect, deserialize_as_seq, Connection};
pub use connection::{Column, DataType, PreparedStatement, QueryResult, ResultSet};
pub use connection::{ExportOpts, HttpTransportOpts, ImportOpts};
pub use params::bind;
