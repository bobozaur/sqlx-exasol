//! Module containing data structures used in representing data returned from the database.

mod attributes;
mod error;
mod fetched;
mod prepared_stmt;
mod result;
mod session_info;

use rsa::errors::Error as RsaError;
use rsa::{pkcs1::DecodeRsaPublicKey, RsaPublicKey};
use serde::Deserialize;
use serde_json::Value;

use crate::error::ExaResultExt;
use crate::{column::ExaColumns, error::ExaProtocolError, options::ProtocolVersion};

use sqlx_core::Error as SqlxError;

pub use attributes::{Attributes, ExaAttributes};
pub use error::ExaDatabaseError;
pub use fetched::DataChunk;
pub use prepared_stmt::PreparedStatement;
pub use result::{QueryResult, ResultSet};
pub use session_info::SessionInfo;

/// A response from the Exasol server.
#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum Response {
    #[serde(rename_all = "camelCase")]
    Ok {
        response_data: Option<ResponseData>,
        attributes: Option<Attributes>,
    },
    Error {
        exception: ExaDatabaseError,
    },
}

/// The response data field of the [`Response::Ok`] variant, received on successful execution of a command.
/// This contains the actual data relevant to the executed command, but there's unfortunately
/// no tag to use for deciding the concrete type to deserialize to.
///
/// Therefore, we try to deserialize all the possible fields, and match on
/// various combinations to determine whether we got the expected response or not.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseData {
    statement_handle: Option<u16>,
    parameter_data: Option<Parameters>,
    results: Option<[QueryResult; 1]>,
    num_rows: Option<usize>,
    data: Option<Vec<Vec<Value>>>,
    nodes: Option<Vec<String>>,
    protocol_version: Option<ProtocolVersion>,
    session_id: Option<u64>,
    release_version: Option<String>,
    database_name: Option<String>,
    product_name: Option<String>,
    max_data_message_size: Option<u64>,
    max_identifier_length: Option<u64>,
    max_varchar_length: Option<u64>,
    identifier_quote_string: Option<String>,
    time_zone: Option<String>,
    time_zone_behavior: Option<String>,
    public_key_pem: Option<String>,
}

/// See: https://github.com/exasol/websocket-api/blob/master/docs/commands/createPreparedStatementV1.md
impl TryFrom<ResponseData> for PreparedStatement {
    type Error = SqlxError;

    fn try_from(value: ResponseData) -> Result<Self, Self::Error> {
        match (value.statement_handle, value.parameter_data) {
            (Some(statement_handle), parameter_data) => {
                let columns = match parameter_data {
                    Some(Parameters { columns }) => columns.0,
                    None => Vec::new().into(),
                };

                let prepared_stmt = Self {
                    statement_handle,
                    columns,
                };

                Ok(prepared_stmt)
            }
            _ => Err(ExaProtocolError::UnexpectedResponse("prepared statement"))?,
        }
    }
}

/// See: https://github.com/exasol/websocket-api/blob/master/docs/commands/executeV1.md
impl TryFrom<ResponseData> for QueryResult {
    type Error = SqlxError;

    fn try_from(value: ResponseData) -> Result<Self, Self::Error> {
        value
            .results
            .map(|i| i.into_iter().next().unwrap())
            .ok_or(ExaProtocolError::UnexpectedResponse("query result"))
            .map_err(From::from)
    }
}

/// See: https://github.com/exasol/websocket-api/blob/master/docs/commands/fetchV1.md
impl TryFrom<ResponseData> for DataChunk {
    type Error = SqlxError;

    fn try_from(value: ResponseData) -> Result<Self, Self::Error> {
        match (value.num_rows, value.data) {
            (Some(num_rows), Some(data)) => Ok(DataChunk { num_rows, data }),
            _ => Err(ExaProtocolError::UnexpectedResponse("data chunk"))?,
        }
    }
}

/// See: https://github.com/exasol/websocket-api/blob/master/docs/commands/getHostsV1.md
impl TryFrom<ResponseData> for Vec<String> {
    type Error = SqlxError;

    fn try_from(value: ResponseData) -> Result<Self, Self::Error> {
        value
            .nodes
            .ok_or(ExaProtocolError::UnexpectedResponse("hosts"))
            .map_err(From::from)
    }
}

/// See: https://github.com/exasol/websocket-api/blob/master/docs/commands/loginV3.md
/// and https://github.com/exasol/websocket-api/blob/master/docs/commands/loginTokenV3.md
impl TryFrom<ResponseData> for SessionInfo {
    type Error = SqlxError;

    fn try_from(value: ResponseData) -> Result<Self, Self::Error> {
        match (
            value.protocol_version,
            value.session_id,
            value.release_version,
            value.database_name,
            value.product_name,
            value.max_data_message_size,
            value.max_identifier_length,
            value.max_varchar_length,
            value.identifier_quote_string,
            value.time_zone,
            value.time_zone_behavior,
        ) {
            (
                Some(protocol_version),
                Some(session_id),
                Some(release_version),
                Some(database_name),
                Some(product_name),
                Some(max_data_message_size),
                Some(max_identifier_length),
                Some(max_varchar_length),
                Some(identifier_quote_string),
                Some(time_zone),
                Some(time_zone_behavior),
            ) => Ok(SessionInfo {
                protocol_version,
                session_id,
                release_version,
                database_name,
                product_name,
                max_data_message_size,
                max_identifier_length,
                max_varchar_length,
                identifier_quote_string,
                time_zone,
                time_zone_behavior,
            }),
            _ => Err(ExaProtocolError::UnexpectedResponse("session info"))?,
        }
    }
}

/// See: https://github.com/exasol/websocket-api/blob/master/docs/commands/loginV3.md
impl TryFrom<ResponseData> for RsaPublicKey {
    type Error = SqlxError;

    fn try_from(value: ResponseData) -> Result<Self, Self::Error> {
        let public_key = value
            .public_key_pem
            .as_ref()
            .ok_or(ExaProtocolError::UnexpectedResponse("public key"))?;

        RsaPublicKey::from_pkcs1_pem(public_key)
            .map_err(RsaError::from)
            .to_sqlx_err()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Parameters {
    pub(crate) columns: ExaColumns,
}
