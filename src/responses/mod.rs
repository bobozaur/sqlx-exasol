mod error;
pub(crate) mod fetched;
mod login_info;
pub(crate) mod prepared_stmt;
pub(crate) mod result;

use rsa::{pkcs1::DecodeRsaPublicKey, RsaPublicKey};
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{column::ExaColumns, options::ProtocolVersion};

use self::{
    fetched::DataChunk, login_info::LoginInfo, prepared_stmt::PreparedStatement,
    result::QueryResult,
};

pub use error::DatabaseError;

/// Generic response received from the Exasol server
/// This is the first deserialization step
/// Used to determine whether the message
/// is a proper response, or an error
///
/// We're forced to use internal tagging as
/// ok/error responses have different adjacent fields
#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum Response {
    #[serde(rename_all = "camelCase")]
    Ok {
        response_data: Option<ResponseData>,
        attributes: Option<Attributes>,
    },
    Error {
        exception: DatabaseError,
    },
}

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

impl TryFrom<ResponseData> for PreparedStatement {
    type Error = String;

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
            _ => Err("can't convert to prepared statement".to_owned()),
        }
    }
}

impl TryFrom<ResponseData> for QueryResult {
    type Error = String;

    fn try_from(value: ResponseData) -> Result<Self, Self::Error> {
        value
            .results
            .map(|i| i.into_iter().next().unwrap())
            .ok_or_else(|| "can't convert to query result".to_owned())
    }
}

impl TryFrom<ResponseData> for DataChunk {
    type Error = String;

    fn try_from(value: ResponseData) -> Result<Self, Self::Error> {
        match (value.num_rows, value.data) {
            (Some(num_rows), Some(data)) => Ok(DataChunk { num_rows, data }),
            _ => Err("can't convert to data chunk".to_owned()),
        }
    }
}

impl TryFrom<ResponseData> for Vec<String> {
    type Error = String;

    fn try_from(value: ResponseData) -> Result<Self, Self::Error> {
        value
            .nodes
            .ok_or_else(|| "can't convert to hosts".to_owned())
    }
}

impl TryFrom<ResponseData> for LoginInfo {
    type Error = String;

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
            ) => Ok(LoginInfo {
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
            _ => Err("can't conver to login info".to_owned()),
        }
    }
}

impl TryFrom<ResponseData> for RsaPublicKey {
    type Error = String;

    fn try_from(value: ResponseData) -> Result<Self, Self::Error> {
        let public_key = value
            .public_key_pem
            .as_ref()
            .ok_or_else(|| "can't convert to public key".to_owned())?;

        RsaPublicKey::from_pkcs1_pem(public_key).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Parameters {
    pub(crate) columns: ExaColumns,
}

/// Struct representing attributes returned from Exasol.
/// These can either be returned by an explicit `getAttributes` call
/// or as part of any response.
///
/// Note that some of these are *read-only*!
/// See the [specification](<https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV1.md#attributes-session-and-database-properties>)
/// for more details.
#[allow(dead_code)]
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Attributes {
    // Read-write attributes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) autocommit: Option<bool>,
    pub(crate) current_schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) feedback_interval: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) numeric_characters: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) query_timeout: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) snapshot_transactions_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timestamp_utc_enabled: Option<bool>,
    // Read-only attributes
    #[serde(skip_serializing)]
    pub(crate) compression_enabled: Option<bool>,
    #[serde(skip_serializing)]
    pub(crate) date_format: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) date_language: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) datetime_format: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) default_like_escape_character: Option<String>,
    #[serde(skip_serializing)]
    #[serde(default)]
    #[serde(deserialize_with = "Attributes::deserialize_open_transaction")]
    pub(crate) open_transaction: Option<bool>,
    #[serde(skip_serializing)]
    pub(crate) timezone: Option<String>,
    #[serde(skip_serializing)]
    pub(crate) timezone_behavior: Option<String>,
}

impl Attributes {
    pub fn update(&mut self, other: Self) {
        macro_rules! other_or_prev {
            ($field:tt) => {
                if let Some(new) = other.$field {
                    self.$field = Some(new);
                }
            };
        }

        other_or_prev!(autocommit);
        other_or_prev!(current_schema);
        other_or_prev!(feedback_interval);
        other_or_prev!(numeric_characters);
        other_or_prev!(query_timeout);
        other_or_prev!(snapshot_transactions_enabled);
        other_or_prev!(timestamp_utc_enabled);
        other_or_prev!(compression_enabled);
        other_or_prev!(date_format);
        other_or_prev!(date_language);
        other_or_prev!(datetime_format);
        other_or_prev!(default_like_escape_character);
        other_or_prev!(open_transaction);
        other_or_prev!(timezone);
        other_or_prev!(timezone_behavior);
    }

    fn deserialize_open_transaction<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let Some(value) = Option::deserialize(deserializer)? else {return Ok(None)};

        match value {
            0 => Ok(Some(false)),
            1 => Ok(Some(true)),
            v => Err(D::Error::custom(format!(
                "Invalid value for 'open_transaction' field: {v}"
            ))),
        }
    }
}
