//! Module containing data structures used in representing data returned from the database.

mod attributes;
mod columns;
mod error;
mod fetch;
mod hosts;
mod prepared_stmt;
mod public_key;
mod result;
mod session_info;

use serde::Deserialize;

pub use attributes::{Attributes, ExaAttributes};
pub use error::ExaDatabaseError;
pub use fetch::DataChunk;
pub use hosts::Hosts;
pub use prepared_stmt::PreparedStatement;
pub use public_key::PublicKey;
pub use result::{QueryResult, ResultSet, ResultSetOutput, Results};
pub use session_info::SessionInfo;

/// A response from the Exasol server.
#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum Response<T> {
    #[serde(rename_all = "camelCase")]
    Ok {
        response_data: T,
        attributes: Option<Attributes>,
    },
    Error {
        exception: ExaDatabaseError,
    },
}
